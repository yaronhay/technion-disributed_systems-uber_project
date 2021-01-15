package server;

import com.google.protobuf.Empty;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uber.proto.objects.Ride;
import uber.proto.rpc.ShardCommunicationGrpc;
import utils.Utils;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class RPCShardCommunicationService extends ShardCommunicationGrpc.ShardCommunicationImplBase {
    static final Logger log = LogManager.getLogger();

    private final ShardServer server;

    public RPCShardCommunicationService(ShardServer server) {
        this.server = server;
        this.receivedRides = new ConcurrentHashMap<>(); // Todo clear sometime
    }

    public static final int K = 3, L = 5, DELTA = 5;
    final Map<UUID, Boolean> receivedRides;

    void doRideGossip(UUID rideID, Ride request) {
        var shard = this.server.serversInShard();

        log.info("Received ride gossip for ride {}", rideID);
        server.data.addRide(rideID, request);

        IntStream.range(0, K).forEach(i -> {
            var randomNodes = utils.Random.getRandomKeys(shard, L);
            log.info("Starting gossip round {} for ride {} to {}", i, rideID, randomNodes);

            var latch = new CountDownLatch(randomNodes.size());
            var streamObserverFactory
                    = rideGossipCallStreamObserver(rideID, i + 1, latch);
            for (var nodeID : randomNodes) {
                var node = server
                        .rpcClient.getShardServerStub(nodeID);
                log.debug("Gossiping ride (iteration {}) {} with {}", i, rideID, nodeID);

                Context.current().fork().run(
                        () -> node.gossipRide(request, streamObserverFactory.apply(nodeID))
                );
            }

            Utils.sleep(DELTA);
            try {
                latch.await();
            } catch (InterruptedException e) { }
            log.info("Finished gossip round {} for ride {} to {}", i, rideID, randomNodes);

        });
    }

    private Function<UUID, StreamObserver<Empty>> rideGossipCallStreamObserver(UUID rideID, int i, CountDownLatch latch) {
        return (UUID nodeID) -> new StreamObserver<>() {
            final AtomicBoolean wasRun = new AtomicBoolean(false);
            private void countDown() {
                if (!wasRun.getAndSet(true)) {
                    latch.countDown();
                }
            }
            @Override public void onNext(Empty empty) {
                log.debug("Gossiping ride (iteration {}) {} with {} completed", i, rideID, nodeID);
            }
            @Override public void onError(Throwable throwable) {
                log.error("Gossiping ride (iteration {}) {} with {} ended with an error : \n {}", i, rideID, nodeID, throwable);
                this.countDown();
            }
            @Override public void onCompleted() {
                log.debug("Gossiping ride (iteration {}) {} with {} completed", i, rideID, nodeID);
                this.countDown();
            }
        };
    }

    @Override public void gossipRide(Ride request, StreamObserver<Empty> responseObserver) {
        UUID rideID = utils.UUID.fromID(request.getId());
        log.debug("Received ride gossip for ride {} from {}", rideID, "?");

        var seen = receivedRides.putIfAbsent(rideID, true) != null;
        if (!seen) {
            this.doRideGossip(rideID, request);
        }

        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }
}
