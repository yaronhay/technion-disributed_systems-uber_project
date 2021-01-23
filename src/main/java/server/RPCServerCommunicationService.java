package server;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import uber.proto.objects.City;
import uber.proto.rpc.*;
import utils.AbortableCountDownLatch;
import utils.UUID;

import java.util.concurrent.ConcurrentHashMap;

public class RPCServerCommunicationService extends ServerCommunicationGrpc.ServerCommunicationImplBase {

    static final Logger log = LogManager.getLogger();

    private final ShardServer server;

    public RPCServerCommunicationService(ShardServer server) {
        this.server = server;
    }

    @Override public void offerRides(OfferRidesRequest request, StreamObserver<OfferRidesResponse> responseObserver) {
        var transactionID = utils.UUID.fromID(request.getTransactionID());
        var coordinatorID = utils.UUID.fromID(request.getServerID());
        var shardID = utils.UUID.fromID(request.getShardID());

        var date = request.getDate();
        var hops = request.getHopsList();

        var res = server.data.offerPath(date, hops, transactionID, shardID, coordinatorID);
        var offers = res.getValue0();
        var seats = res.getValue1();
        var locks = res.getValue2();
        var rides = res.getValue3();

        var ridesResponse = OfferRidesResponse.newBuilder();
        for (int i = 0; i < offers.length; i++) {
            if (offers[i] == null) {
                continue;
            }

            City src = hops.get(i).getSrc(), dst = hops.get(i).getDst();

            var offer = RideOffer.newBuilder()
                    .setRideID(UUID.toID(offers[i]))
                    .setRideInfo(rides[i])
                    .setSeat(seats[i])
                    .setLock(locks[i])
                    .build();
            ridesResponse.putOffers(i + 1, offer);

            log.info("Adding offer ride {}_{} as (Transaction ID {}) for hop from {} to {} ",
                    offers[i], seats[i], transactionID,
                    server.cityName.get(utils.UUID.fromID(src.getId())),
                    server.cityName.get(utils.UUID.fromID(dst.getId())));

        }
        responseObserver.onNext(ridesResponse.build());
        responseObserver.onCompleted();
        log.info("Finished sending offers (Transaction ID {})", transactionID);
    }

    @Override public void releaseSeats(ReleaseSeatsRequest request,
                                       StreamObserver<ReleaseSeatsResponse> responseObserver) {
        var transactionID = utils.UUID.fromID(request.getTransactionID());
        var offers = request.getOffersList();
        for (RideOffer offer : offers) {
            java.util.UUID ride_id = UUID.fromID(offer.getRideID());
            int seat = offer.getSeat();
            String lock = offer.getLock();

            try {
                // Todo remove from remove listener
                this.server.releaseLockSeat(ride_id, seat, lock,
                        String.format("Release due to server request (Transaction ID %s)", transactionID));
            } catch (InterruptedException | KeeperException e) {
                log.error("Failed to release lock {} (Transaction ID {}) for seat {} ride {}",
                        lock, transactionID, seat, ride_id);
            }
        }
    }

    @Override public StreamObserver<SnapshotRequest> sendSnapshot(StreamObserver<SnapshotResponse> responseObserver) {
        return new StreamObserver<SnapshotRequest>() {
            private StreamObserver<UberSnapshotResponse> streamObserver;
            private boolean started = false;
            private java.util.UUID snapshotID;
            private AbortableCountDownLatch latch;

            @Override public void onNext(SnapshotRequest snapshotRequest) {
                if (started) {
                    synchronized (streamObserver) {
                        streamObserver.onNext(UberSnapshotResponse
                                .newBuilder()
                                .setRideStatus(snapshotRequest.getRideStatus())
                                .build());
                    }
                } else {
                    if (snapshotRequest.hasSnapshotID()) {
                        started = true;
                        snapshotID = UUID.fromID(snapshotRequest.getSnapshotID());
                        var snapshotInfo = server.snapshotInfo.get(snapshotID);
                        this.latch = snapshotInfo.latch;
                        this.streamObserver = snapshotInfo.streamObserver;
                    }
                }

            }
            @Override public void onError(Throwable throwable) {
                latch.abort();
            }
            @Override public void onCompleted() {
                latch.countDown();
            }
        };
    }
}
