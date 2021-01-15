package server;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.ranges.Range;
import uber.proto.objects.ID;
import uber.proto.objects.Ride;
import uber.proto.objects.User;
import uber.proto.rpc.PlanPathRequest;
import uber.proto.rpc.PlanPathResponse;
import uber.proto.rpc.UberRideServiceGrpc;
import utils.UUID;

import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

        var hops = request.getHopsList();
        var date = request.getDate();

        var response = PlanPathResponse.newBuilder();
        response.addAllRides(
                hops.stream().map(
                        hop -> Ride.newBuilder()
                                    .setId(UUID.toID(UUID.generate()))
                                    .setProvider(User.newBuilder()
                                            .setFirstName("Rab")
                                            .setLastName("hay")
                                            .setPhoneNumber("055").build())
                                    .setDate(date)
                                    .setSource(server.getCityByID(hop.getSrc().getId()))
                                    .setDestination(server.getCityByID(hop.getDst().getId()))
                                    .build()

                ).collect(Collectors.toList())
        );
        var success = true;
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


}
