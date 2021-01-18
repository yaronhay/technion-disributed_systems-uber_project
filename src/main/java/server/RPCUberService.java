package server;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uber.proto.objects.*;
import uber.proto.rpc.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

public class RPCUberService extends UberRideServiceGrpc.UberRideServiceImplBase {

    static final Logger log = LogManager.getLogger();

    private final ShardServer server;

    public RPCUberService(ShardServer server) {
        this.server = server;
    }

    @Override
    public void addRide(Ride request, StreamObserver<ID> responseObserver) {
        var id = utils.UUID.generate();
        ID rideID = utils.UUID.toID(id);
        request = request.toBuilder().setId(rideID).build();

        var selfStub = this.server.rpcClient.getShardServerStub(this.server.id);

        log.debug("Received a new ride to add. Assigned ride id of {}", id);
        log.debug("Starting a ride gossip of ride {} with self", id);
        selfStub.gossipRide(request, new StreamObserver<Empty>() {
            @Override public void onNext(Empty empty) { }
            @Override public void onError(Throwable throwable) {
                log.error("The ride gossip of ride {} with self ended with an error", id);
                responseObserver.onNext(ID.newBuilder().build());
                responseObserver.onCompleted();
            }
            @Override public void onCompleted() {
                log.debug("The ride gossip of ride {} with self finished - returning id as result to caller", id);
                responseObserver.onNext(rideID);
                responseObserver.onCompleted();
            }
        });
    }

    @Override public void planPath(
            PlanPathRequest request, StreamObserver<PlanPathResponse> responseObserver) {
        var transactionID = request.getTransactionID();
        var transactionUUID = utils.UUID.fromID(transactionID);

        log.debug("Received a path planning request (Transaction id {})", transactionUUID);

        OfferRidesRequest offerRidesRequest = OfferRidesRequest.newBuilder()
                .setDate(request.getDate())
                .addAllHops(request.getHopsList())
                .setTransactionID(transactionID)
                .build();


        Map<UUID, UUID> servers = new HashMap<>();
        for (var k : server.shardsServers.entrySet()) {
            var shardID = k.getKey();
            var serverID = utils.Random.getRandomKey(k.getValue());
            servers.put(shardID, serverID);
        }

        OfferCollector offerCollector = new OfferCollector(request.getHopsCount(), servers.size());
        for (var k : servers.entrySet()) {
            var shardID = k.getKey();
            var serverID = k.getValue();

            var stub = server.rpcClient.getServerStub(shardID, serverID);
            stub.offerRides(offerRidesRequest,
                    offerCollector.collector(shardID, serverID, transactionUUID));
        }
        offerCollector.waitToFinish();

        var offersOK = true;
        for (var i = 0; i < offerCollector.offers.length(); i++) {
            var offer = offerCollector.offers.get(i);
            if (offer == null) {
                offersOK = false;
                break;
            }
        }

        Map<UUID, ReleaseSeatsRequest> toRelease;
        if (offersOK) {
            log.info("Found (Transaction ID {}) a good offer collection of rides that satisfies the path : {} ",
                    transactionUUID, offerCollector.offers);
            toRelease = offerCollector.getSeatsToRelease();

        } else {
            log.info("Offers (Transaction ID {}) did not satisfy the path : {} ",
                    transactionUUID, offerCollector.offers);
            toRelease = offerCollector.getAllSeatsToRelease();
        }
        // Todo release
        var response = PlanPathResponse.newBuilder();
        if (!offersOK) {
            response.setSuccess(false);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
            return;
        }

//        response.addAllRides(
//                hops.stream().map(
//                        hop -> Ride.newBuilder()
//                                .setId(utils.UUID.toID(utils.UUID.generate()))
//                                .setProvider(User.newBuilder()
//                                        .setFirstName("Rab")
//                                        .setLastName("hay")
//                                        .setPhoneNumber("055").build())
//                                .setDate(date)
//                                .setSource(server.getCityByID(hop.getSrc().getId()))
//                                .setDestination(server.getCityByID(hop.getDst().getId()))
//                                .build()
//
//                ).collect(Collectors.toList())
//        );

        // Todo actual lock
        var success = offersOK;
        if (success) {
            response.setSuccess(true);
            log.debug("The path planning (Transaction ID {}) with self finished - returning result to caller", transactionUUID);
        } else {
            response.setSuccess(false);
            log.debug("The path planning (Transaction ID {}) with self failed", transactionUUID);
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    class OfferCollector {
        class Offer {
            public ID id;
            public UUID shardID;
            public UUID serverID;
            public int seat;
            public String s;
            @Override public String toString() {
                return String.format("Offer(%s, rideid%s, shard%s)", s, utils.UUID.fromID(id), shardID);
            }
        }

        final Object lock;
        //ID[] offers;
        final List<Offer> toRelease;
        AtomicReferenceArray<Offer> offers;
        //UUID[] shards;
        CountDownLatch latch;
        OfferCollector(int n, int size) {
            lock = new Object();
            //offers = new ID[n];
            //shards = new UUID[n];
            offers = new AtomicReferenceArray<>(n);
            latch = new CountDownLatch(size);
            toRelease = Collections.synchronizedList(new LinkedList<>());
        }

        public Map<UUID, ReleaseSeatsRequest> getSeatsToRelease() {
            Map<UUID, ReleaseSeatsRequest.Builder> res = new HashMap<>();

            synchronized (toRelease) {
                for (var offer : toRelease) {
                    var builder =
                            res.computeIfAbsent(offer.serverID, k -> ReleaseSeatsRequest.newBuilder());
                    builder.addSeats(ReleaseSeatsRequest.Seat
                            .newBuilder()
                            .setRideID(offer.id)
                            .setSeat(offer.seat)
                            .build()
                    );
                }
            }
            for (var i = 0; i < offers.length(); i++) {
                var offer = offers.get(i);
                if (offer == null) {
                    var builder =
                            res.computeIfAbsent(offer.serverID, k -> ReleaseSeatsRequest.newBuilder());
                    builder.addSeats(ReleaseSeatsRequest.Seat
                            .newBuilder()
                            .setRideID(offer.id)
                            .setSeat(offer.seat)
                            .build()
                    );
                }
            }

            return res.entrySet().stream()
                    .filter(e -> e.getValue().getSeatsCount() != 0)
                    .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().build()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));
        }

        public Map<UUID, ReleaseSeatsRequest> getAllSeatsToRelease() {
            Map<UUID, ReleaseSeatsRequest.Builder> res = new HashMap<>();

            synchronized (toRelease) {
                for (var offer : toRelease) {
                    var builder =
                            res.computeIfAbsent(offer.serverID, k -> ReleaseSeatsRequest.newBuilder());
                    builder.addSeats(ReleaseSeatsRequest.Seat
                            .newBuilder()
                            .setRideID(offer.id)
                            .setSeat(offer.seat)
                            .build()
                    );
                }
            }
            return res.entrySet().stream()
                    .filter(e -> e.getValue().getSeatsCount() != 0)
                    .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().build()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));
        }
        StreamObserver<OfferRidesResponse> collector(UUID shardID, UUID serverID, UUID transactionID) {
            return new StreamObserver<>() {
                final AtomicBoolean wasRun = new AtomicBoolean(false);
                private void countDown() {
                    if (!wasRun.getAndSet(true)) {
                        latch.countDown();
                    }
                }

                @Override public void onNext(OfferRidesResponse response) {
                    var offs = response.getOffersMap();

                    if (offs.isEmpty()) {
                        return;
                    }

                    for (var e : offs.entrySet()) {
                        var i = e.getKey() - 1;
                        var val = e.getValue();

                        var offer = new Offer();
                        offer.id = val.getRideID();
                        offer.shardID = shardID;
                        offer.serverID = serverID;
                        offer.seat = val.getSeat();
                        var src = server.getCityByID(val.getSource().getId()).getName();
                        var dst = server.getCityByID(val.getDestination().getId()).getName();
                        offer.s = String.format("%s->%s", src, dst);

                        if (!offers.compareAndSet(i, null, offer)) {
                            toRelease.add(offer);
                        }
                    }

                }
                @Override public void onError(Throwable throwable) {
                    log.error("Receiving ride offers from server {} in shard {}  (Transaction ID {}) ended with an error : \n {}", serverID, shardID, transactionID, throwable);
                    this.countDown();
                }
                @Override public void onCompleted() {
                    log.debug("Receiving ride offers from server {} in shard {}  (Transaction ID {}) completed", serverID, shardID, transactionID);
                    this.countDown();
                }
            };
        }

        void waitToFinish() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
