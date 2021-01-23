package server;


import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.javatuples.Pair;
import uber.proto.objects.*;
import uber.proto.objects.Date;
import uber.proto.rpc.PlanPathRequest;
import uber.proto.rpc.SnapshotRequest;
import utils.Utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CityRides {
    final static Logger log = LogManager.getLogger();

    private Map<UUID, PlanPathRequest> paths;
    private Map<UUID, List<User>> reservations;
    private Map<String, List<UUID>> schedule;
    private Map<UUID, Ride> rides;

    final ShardServer server;

    List<User> getReservations(UUID rideID, int minSize) {
        var l = reservations.computeIfAbsent(rideID,
                id -> Collections.synchronizedList(new LinkedList<User>()));
        if (minSize > 0) {
            synchronized (l) {
                while (l.size() < minSize) {
                    l.add(null);
                }
            }
        }
        return l;
    }

    List<UUID> getSchedule(String date) {
        return schedule.computeIfAbsent(date,
                id -> Collections.synchronizedList(new LinkedList<UUID>()));
    }

    public CityRides(ShardServer server) {
        this.reservations = new ConcurrentHashMap<>();
        this.schedule = new ConcurrentHashMap<>();
        this.rides = new ConcurrentHashMap<>();
        this.paths = new ConcurrentHashMap<>();
        this.server = server;
    }

    public void addRide(UUID rideID, Ride ride) {
        var dateKey = Utils.dateAsStr(ride.getDate());
        this.rides.putIfAbsent(rideID, ride);
        this.getSchedule(dateKey).add(rideID);
        this.getReservations(rideID, ride.getVacancies());
    }

    public void addPath(UUID transactionID, PlanPathRequest path) {
        this.paths.putIfAbsent(transactionID, path);
    }

    public boolean hasRide(UUID rideID) {
        return this.rides.containsKey(rideID);
    }

    public void addReservation(UUID rideID, int seat, User consumer) {
        var reservations = getReservations(rideID, seat);
        synchronized (reservations) {
            var seatIdx = seat - 1;
            var data = reservations.get(seatIdx);
            if (data != null) {
                log.error("Double Reservation Error: Seat number {} in ride {} is taken by User({}, {}, {})\nCannot reserve seat for User({}, {}, {})",
                        seat, rideID,
                        data.getFirstName(), data.getLastName(), data.getPhoneNumber(),
                        consumer.getFirstName(), consumer.getLastName(), consumer.getPhoneNumber());
                throw new IllegalStateException("Double reservation for a seat is illegal");
            }
            reservations.set(seat - 1, consumer);
        }

    }

    public void sendSnapshot(StreamObserver<SnapshotRequest> streamObserver) {
        for (var rideID : rides.keySet()) {
            var ride = this.rides.get(rideID);

            var reservations = getReservations(rideID, 0);
            var reservationsMap = IntStream.range(0, reservations.size())
                    .filter(i -> reservations.get(i) != null)
                    .mapToObj(i -> Pair.with(i + 1, reservations.get(i)))
                    .collect(Collectors.toMap(Pair::getValue0, Pair::getValue1));

            var rideStatus = RideStatus
                    .newBuilder()
                    .setRide(ride)
                    .putAllReservations(reservationsMap)
                    .build();
            streamObserver.onNext(SnapshotRequest
                    .newBuilder()
                    .setRideStatus(rideStatus)
                    .build());
        }
    }

    class RideTestInfo {
        public String s1, s2;
    }
    public boolean offerRides(Date date, List<Hop> hops, UUID[] offers, int[] seats, Ride[] rides, String[] locks, UUID transactionID, UUID shardID, UUID serverID) {
        List<Integer> emptyHops = new LinkedList<>();
        for (int i = 0; i < offers.length; i++) {
            if (offers[i] == null) {
                emptyHops.add(i);
            }
        }

        String dateKey = Utils.dateAsStr(date);
        var schedule = this.getSchedule(dateKey);
        synchronized (schedule) {
            var it = schedule.iterator();

            while (it.hasNext() && !emptyHops.isEmpty()) {
                var rideID = it.next();
                var ride = this.rides.get(rideID);

                Integer remove = null;
                for (var emptyHopIdx : emptyHops) {
                    var emptyHop = hops.get(emptyHopIdx);

                    var info = new RideTestInfo();
                    if (goodOffer(ride, emptyHop, info)) {
                        var limit = 0;
                        boolean breakExteralLoop = false;
                        while (true) {
                            var seat = getEmptySeat(rideID, ride, info, limit);
                            limit = seat;
                            if (seat != 0) {
                                boolean locked;
                                String lock = null;
                                try {
                                    lock = this.server.tryLockSeat(rideID, seat, transactionID);
                                    locked = lock != null;
                                } catch (KeeperException | InterruptedException e) {
                                    log.error("Exception when trying to lock {}_{}", rideID, seat, e);
                                    locked = false;
                                }

                                if (locked) {
                                    server.serversWatcher.addWatchRemoveGroup(serverID, transactionID);
                                    var finalLock = lock;
                                    server.serversWatcher.addWatchRemove(transactionID, () -> {
                                        try {
                                            server.releaseLockSeat(rideID, seat, finalLock,
                                                    String.format("Release due to server failure (Transaction ID %s)", transactionID));
                                        } catch (InterruptedException | KeeperException e) {
                                            log.error("Exception when trying to release lock {} on {}_{}", finalLock, rideID, seat, e);
                                        }
                                    });
                                    offers[emptyHopIdx] = rideID;
                                    seats[emptyHopIdx] = seat;
                                    locks[emptyHopIdx] = lock;
                                    rides[emptyHopIdx] = ride;
                                    remove = emptyHopIdx;
                                    log.debug("Checking ride (Transaction ID {})\n\t{}\n\t{}\n\tLocked!",
                                            transactionID, info.s1, info.s2);
                                    breakExteralLoop = true;
                                    break; // for external loop
                                } else {
                                    log.debug("Checking ride (Transaction ID {})\n\t{}\n\t{}\n\tFailed to lock",
                                            transactionID, info.s1, info.s2);
                                }
                            } else {
                                break;
                            }
                        }
                        if (breakExteralLoop) {
                            break;
                        }

                    }
                    log.debug("Checking ride (Transaction ID {})\n\t{}\n\t{}",
                            transactionID, info.s1, info.s2);
                }

                if (remove != null) {
                    emptyHops.remove(remove);
                }
            }
        }

        return emptyHops.isEmpty();
    }

    int getEmptySeat(UUID rideID, Ride ride, RideTestInfo info, int limit) {
        var reservations = getReservations(rideID, ride.getVacancies());
        int seat = 0;

        synchronized (reservations) {
            int i = 0;
            for (User user : reservations) {
                i++;
                if (user == null && i > limit) {
                    seat = i;
                    break;
                }
            }
        }
        var rideSrc = utils.UUID.fromID(ride.getSource().getId());
        var rideDst = utils.UUID.fromID(ride.getDestination().getId());
        if (seat != 0) {
            info.s2 = String.format("Found an empty - seat no. %d in ride %s -> %s",
                    seat, server.cityName.get(rideSrc), server.cityName.get(rideDst)
            );
        } else {
            info.s2 = String.format("Didn't find an empty - seat no. %d in ride %s -> %s",
                    seat, server.cityName.get(rideSrc), server.cityName.get(rideDst)
            );
        }
        return seat;
    }

    boolean goodOffer(Ride ride, Hop hop, RideTestInfo info) {
        var hopSrc = utils.UUID.fromID(hop.getSrc().getId());
        var hopDst = utils.UUID.fromID(hop.getDst().getId());

        var rideSrc = utils.UUID.fromID(ride.getSource().getId());
        var rideDst = utils.UUID.fromID(ride.getDestination().getId());

        var rideSrcLoc = server.cityLoc.get(rideSrc);
        var rideDstLoc = server.cityLoc.get(rideDst);

        if (hopSrc.equals(rideSrc) || hopDst.equals(rideDst)) {
            City.Location point;
            if (hopSrc.equals(rideSrc)) {
                point = server.cityLoc.get(hopDst);

            } else {
                point = server.cityLoc.get(hopSrc);
            }

            var distance = utils.Utils.distancePointSegment(
                    rideSrcLoc.getX(), rideSrcLoc.getY(),
                    rideDstLoc.getX(), rideDstLoc.getY(),
                    point.getX(), point.getY()
            );


            var answer = distance <= ride.getPermittedDeviation();
            if (answer) {
                info.s1 = String.format(
                        "Found a good offer %s -> %s (PD = %f) for hop %s -> %s Distance of %f from line segment",
                        server.cityName.get(rideSrc), server.cityName.get(rideDst), ride.getPermittedDeviation(),
                        server.cityName.get(hopSrc), server.cityName.get(hopDst), distance
                );
            } else {
                info.s1 = String.format(
                        "Didn't find a good offer %s -> %s (PD = %f) for hop %s -> %s Distance of %f from line segment",
                        server.cityName.get(rideSrc), server.cityName.get(rideDst), ride.getPermittedDeviation(),
                        server.cityName.get(hopSrc), server.cityName.get(hopDst), distance
                );
            }
            return answer;
        }
        info.s1 = String.format(
                "Didn't find a good offer %s -> %s (PD = %f) for hop %s -> %s because no src / dst match",
                server.cityName.get(rideSrc), server.cityName.get(rideDst), ride.getPermittedDeviation(),
                server.cityName.get(hopSrc), server.cityName.get(hopDst)
        );
        return false;
    }

}