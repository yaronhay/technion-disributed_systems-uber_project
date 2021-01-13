package server;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uber.proto.objects.ID;
import uber.proto.objects.Ride;
import uber.proto.rpc.UberRideServiceGrpc;
import uber.proto.rpc.UberShardCommunicationGrpc;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RPCShardCommunicationService extends UberShardCommunicationGrpc.UberShardCommunicationImplBase {
    static final Logger log = LogManager.getLogger();

    private final ShardServer server;

    public RPCShardCommunicationService(ShardServer server) {
        this.server = server;
        this.receivedRides = new ConcurrentHashMap<>();
    }

    public static final int K = 3, L = 5, DELTA = 5;
    final Map<UUID, Boolean> receivedRides;
    @Override public void gossipRide(Ride request, StreamObserver<Empty> responseObserver) {
        UUID rideID = utils.UUID.fromID(request.getId());
        UUID srcCityID = utils.UUID.fromID(request.getSource().getId());

        var seen = receivedRides.putIfAbsent(rideID, true) != null;
        if (!seen) {
            log.info("Received ride gossip for ride {}", rideID.toString());
            server.data.get(srcCityID).addRide(rideID, request);

            var shard = this.server.serversInShard();
            for (int i = 0; i < K; i++) {
                var nodeIDs = utils.Random.getRandomKeys(shard, L);
                log.info("Starting a gossip round for ride {} to {}",
                        rideID.toString(), nodeIDs.toString());
                for (var nodeID : nodeIDs) {
                    var node = server.rpcClient.getShardServerStub(nodeID);
                    node.gossipRide(request, null);
                }
                try {
                    Thread.sleep(DELTA * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

        responseObserver.onCompleted();
    }
}
