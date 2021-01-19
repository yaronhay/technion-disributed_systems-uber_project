package server;


import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Triplet;
import uber.proto.objects.Date;
import uber.proto.objects.Hop;
import uber.proto.objects.Ride;
import uber.proto.objects.User;
import uber.proto.rpc.SnapshotRequest;
import utils.Utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ShardData {
    static final Logger log = LogManager.getLogger();

    private final Map<UUID, CityRides> cities;

    // Will be used in the opposite way (ONE READER and MULTIPLE WRITERS)
    private final ReadWriteLock lock;
    final ShardServer server;

    public ShardData(ShardServer server) {
        this.server = server;
        this.cities = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock(true);
    }

    CityRides get(UUID city) {
        return this.cities.computeIfAbsent(city, c -> new CityRides(server));
    }

    public void addRide(UUID rideID, Ride ride) {
        this.lock.readLock().lock();
        try {
            UUID cityID = utils.UUID.fromID(ride.getSource().getId());
            CityRides cityRides = this.get(cityID);

            if (!cityRides.hasRide(rideID)) {
                cityRides.addRide(rideID, ride);
            }
            var src = server.getCityByID(ride.getSource().getId()).getName();
            var dst = server.getCityByID(ride.getDestination().getId()).getName();
            log.info("Added ride {} -> {} on {} #{} to local database",
                    src, dst, Utils.dateAsStr(ride.getDate()), rideID);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public Quartet<UUID[], int[], String[], Ride[]> offerPath(Date date, List<Hop> hops, UUID transactionID, UUID shardID, UUID serverID) {
        UUID[] offers = new UUID[hops.size()];
        Ride[] rides = new Ride[hops.size()];
        int[] seats = new int[hops.size()];
        String[] locks = new String[hops.size()];
        this.lock.writeLock().lock();
        try {
            for (var city : cities.values()) {
                if (city.offerRides(date, hops, offers, seats,rides, locks, transactionID, shardID, serverID)) {
                    break;
                }
            }
            return Quartet.with(offers, seats, locks, rides);
        } finally {
            this.lock.writeLock().unlock();
        }
    }
    public void reserveSeat(UUID srcCity, UUID rideID, int seat, User consumer) {
        this.lock.readLock().lock();
        try {
            var cityRides = this.cities.get(srcCity);
            cityRides.addReservation(rideID, seat, consumer);

            log.info("Updated local database with a reservation of seat {} in ride {} for User({}, {}, {})",
                    seat, rideID, consumer.getFirstName(), consumer.getLastName(), consumer.getPhoneNumber());
        } finally {
            this.lock.readLock().unlock();
        }

    }
    public void sendSnapshot(StreamObserver<SnapshotRequest> streamObserver) {
        this.lock.writeLock().lock();
        try {
            for (var city : cities.values()) {
                city.sendSnapshot(streamObserver);
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }
}
