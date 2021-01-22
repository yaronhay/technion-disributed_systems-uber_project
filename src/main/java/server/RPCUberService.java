package server;

import com.google.protobuf.Empty;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Context;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.zookeeper.KeeperException;
import org.javatuples.Pair;
import uber.proto.objects.*;
import uber.proto.rpc.*;
import uber.proto.zk.SnapshotTask;
import utils.AbortableCountDownLatch;

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

    @Override public void addRide(Ride request, StreamObserver<ID> responseObserver) {
        var id = utils.UUID.generate();
        ID rideID = utils.UUID.toID(id);
        request = request.toBuilder()
                .setId(rideID)
                .build();


        log.debug("Received a new ride to add. Assigned ride id of {}", id);
        if (this.server.atomicAddRide(id, request)) {
            log.debug("The atomic add ride ({}) finished - returning id as result to caller", id);
            responseObserver.onNext(rideID);
        } else {
            log.error("The atomic add ride ({}) ended with an error", id);
            responseObserver.onNext(ID.newBuilder().build());
        }
        responseObserver.onCompleted();
    }

    @Override public void addRideGossip(Ride request, StreamObserver<ID> responseObserver) {
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

        OfferRidesRequest offerRidesRequest = makeOfferRidesRequest(request, transactionID);
        Map<UUID, UUID> servers = server.getRandomServersForAllShards();

        OfferCollector offerCollector = new OfferCollector(
                request.getHopsCount(),
                servers.size(),
                transactionID);

        if (!getOffers(transactionUUID, offerRidesRequest, servers, offerCollector)) {
            responseObserver.onNext(PlanPathResponse.newBuilder()
                    .setSuccess(false)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        var success = this.server.atomicSeatsReserve(
                offerCollector.offers,
                request.getConsumer(),
                transactionUUID);

        var response = PlanPathResponse.newBuilder();
        if (!success) {
            log.debug("Atomic reservation of offers (Transaction ID {}) failed", transactionUUID);
            response.setSuccess(false);
            log.debug("The path planning (Transaction ID {}) with self failed", transactionUUID);
        } else {
            response.setSuccess(true);
            for (var i = 0; i < offerCollector.offers.length(); i++) {
                var offer = offerCollector.offers.get(i);
                response.addRides(offer.rideOffer.getRideInfo());
            }
            log.debug("The path planning (Transaction ID {}) with self finished - returning result to caller", transactionUUID);
        }

        releaseLocks(transactionUUID, servers, offerCollector.getOffersSeatsToRelease());

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }
    private boolean getOffers(UUID transactionUUID,
                              OfferRidesRequest offerRidesRequest,
                              Map<UUID, UUID> servers,
                              OfferCollector offerCollector) {
        boolean offersOK = collectOffers(
                transactionUUID,
                offerRidesRequest,
                servers,
                offerCollector);

        Map<UUID, ReleaseSeatsRequest> toRelease;
        if (offersOK) {
            log.info("Found (Transaction ID {}) a good offer collection of rides that satisfies the path : {} ",
                    transactionUUID, offerCollector.offers);
            toRelease = offerCollector.getExceedingSeatsToRelease();

        } else {
            log.info("Offers (Transaction ID {}) did not satisfy the path : {} ",
                    transactionUUID, offerCollector.offers);
            toRelease = offerCollector.getAllSeatsToRelease();
        }

        releaseLocks(transactionUUID, servers, toRelease);


        return offersOK;
    }
    private OfferRidesRequest makeOfferRidesRequest(PlanPathRequest request, ID transactionID) {
        OfferRidesRequest offerRidesRequest = OfferRidesRequest.newBuilder()
                .setDate(request.getDate())
                .addAllHops(request.getHopsList())
                .setTransactionID(transactionID)
                .setServerID(utils.UUID.toID(this.server.id))
                .setShardID(utils.UUID.toID(this.server.shard))
                .build();
        return offerRidesRequest;
    }
    private boolean collectOffers(UUID transactionUUID, OfferRidesRequest offerRidesRequest, Map<UUID, UUID> servers, OfferCollector offerCollector) {
        for (var k : servers.entrySet()) {
            var shardID = k.getValue();
            var serverID = k.getKey();

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
        return offersOK;
    }

    private void releaseLocks(UUID transactionID, Map<UUID, UUID> servers, Map<UUID, ReleaseSeatsRequest> toRelease) {
        for (var k : toRelease.entrySet()) {
            var serverID = k.getKey();
            var releaseSeatsRequest = k.getValue();
            var shardID = servers.get(serverID);


            var stub = server.rpcClient.getServerStub(shardID, serverID);
            StreamObserver<ReleaseSeatsResponse> releaseObserver = new StreamObserver<>() {
                @Override public void onNext(ReleaseSeatsResponse releaseSeatsResponse) { }
                @Override public void onError(Throwable throwable) {
                    log.error("Release seats (Transaction ID {}) to server {} in shard {} ended with an error:\n{}",
                            transactionID, server, shardID, throwable);
                }
                @Override public void onCompleted() {
                    log.debug("Release seats (Transaction ID {}) to server {} in shard {} completed",
                            transactionID, server, shardID);
                }
            };
            Context.current().fork().run(
                    () -> stub.releaseSeats(releaseSeatsRequest, releaseObserver));
        }
    }

    class OfferCollector {
        class Offer {
            public ID id;
            public UUID shardID;
            public UUID serverID;
            public RideOffer rideOffer;
            public String s;
            @Override public String toString() {
                return String.format("Offer(%s, rideid%s, shard%s)", s, utils.UUID.fromID(id), shardID);
            }
        }

        ID transactionID;
        final Object lock;
        final List<Offer> toRelease;
        AtomicReferenceArray<Offer> offers;
        CountDownLatch latch;
        OfferCollector(int n, int size, ID transactionID) {
            this.transactionID = transactionID;
            lock = new Object();
            offers = new AtomicReferenceArray<>(n);
            latch = new CountDownLatch(size);
            toRelease = Collections.synchronizedList(new LinkedList<>());
        }

        public Map<UUID, ReleaseSeatsRequest> getExceedingSeatsToRelease() {
            Map<UUID, ReleaseSeatsRequest.Builder> res = getReleaseBuilderMap();
            addExceedingSeatsToRelease(res);
            return toReleaseSeatsRequestMap(res);
        }
        public Map<UUID, ReleaseSeatsRequest> getOffersSeatsToRelease() {
            Map<UUID, ReleaseSeatsRequest.Builder> res = getReleaseBuilderMap();
            addOfferToReleaseRequest(res);
            return toReleaseSeatsRequestMap(res);
        }
        public Map<UUID, ReleaseSeatsRequest> getAllSeatsToRelease() {
            Map<UUID, ReleaseSeatsRequest.Builder> res = getReleaseBuilderMap();
            addExceedingSeatsToRelease(res);
            addOfferToReleaseRequest(res);
            return toReleaseSeatsRequestMap(res);
        }


        private Map<UUID, ReleaseSeatsRequest.Builder> getReleaseBuilderMap() {
            return new HashMap<>();
        }
        private void addExceedingSeatsToRelease(Map<UUID, ReleaseSeatsRequest.Builder> res) {
            synchronized (toRelease) {
                for (var offer : toRelease) {
                    var builder =
                            res.computeIfAbsent(offer.serverID, k -> ReleaseSeatsRequest.newBuilder());
                    builder.addOffers(offer.rideOffer);
                }
            }
        }
        private void addOfferToReleaseRequest(Map<UUID, ReleaseSeatsRequest.Builder> res) {
            for (var i = 0; i < offers.length(); i++) {
                var offer = offers.get(i);
                if (offer != null) {
                    var builder =
                            res.computeIfAbsent(offer.serverID, k -> ReleaseSeatsRequest.newBuilder());
                    builder.addOffers(offer.rideOffer);
                }
            }
        }
        private Map<UUID, ReleaseSeatsRequest> toReleaseSeatsRequestMap(Map<UUID, ReleaseSeatsRequest.Builder> res) {
            return res.entrySet().stream()
                    .filter(e -> e.getValue().getOffersCount() != 0)
                    .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue()
                            .setTransactionID(transactionID)
                            .build()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }


        StreamObserver<OfferRidesResponse> collector(UUID shardID, UUID serverID, UUID transactionID) {
            return new StreamObserver<>() {
                private List<Offer> offersReceived = new LinkedList<>();
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
                        offer.rideOffer = val;

                        var src = server.getCityByID(val.getRideInfo().getSource().getId()).getName();
                        var dst = server.getCityByID(val.getRideInfo().getDestination().getId()).getName();
                        offer.s = String.format("%s->%s", src, dst);

                        if (!offers.compareAndSet(i, null, offer)) {
                            toRelease.add(offer);
                        }

                        offersReceived.add(offer);
                    }

                }
                @Override public void onError(Throwable throwable) {
                    log.error(new ParameterizedMessage("Receiving ride offers from server {} in shard {}  (Transaction ID {}) ended with an error:", serverID, shardID, transactionID), throwable);
                    this.countDown();
                }
                @Override public void onCompleted() {
                    log.info("Receiving ride offers from server {} in shard {}  (Transaction ID {}) completed:\n\t{}", serverID, shardID, transactionID,
                            offersReceived.stream()
                                    .map(Offer::toString)
                                    .collect(Collectors.joining("\n\t")));
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

    public static class SnapshotInfo {
        public AbortableCountDownLatch latch;
        public StreamObserver<UberSnapshotResponse> streamObserver;
    }
    @Override public void snapshot(UberSnapshotRequest request, StreamObserver<UberSnapshotResponse> responseObserver) {
        var snapshotUUID = utils.UUID.generate();

        // Server ID, Shard ID
        Map<UUID, UUID> servers = server.getRandomServersForAllShards();

        AbortableCountDownLatch latch = new AbortableCountDownLatch(servers.size());
        {
            var snapshotInfo = new SnapshotInfo();
            snapshotInfo.latch = latch;
            snapshotInfo.streamObserver = responseObserver;

            server.snapshotInfo.put(snapshotUUID, snapshotInfo);
        }

        if (!server.startSnapshotTask(snapshotUUID, servers)) {
            synchronized (responseObserver) {
                responseObserver.onNext(UberSnapshotResponse.newBuilder().build());
                responseObserver.onCompleted();
            }
            return;
        }

        for (var serverID : servers.keySet()) {
            server.serversWatcher.addWatchRemoveGroup(serverID, snapshotUUID);
            server.serversWatcher.addWatchRemove(snapshotUUID, latch::abort);
        }

        try {
            latch.await();
        } catch (AbortableCountDownLatch.AbortedException e) {
            Status status = Status.newBuilder()
                    .setCode(Code.ABORTED_VALUE)
                    .setMessage("Some server failed")
                    .build();
            synchronized (responseObserver) {
                responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            }
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        synchronized (responseObserver) {
            responseObserver.onCompleted();
        }
    }
}
