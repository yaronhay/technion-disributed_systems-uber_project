package server;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uber.proto.objects.ID;
import uber.proto.objects.Ride;
import uber.proto.rpc.UberRideServiceGrpc;

import java.util.UUID;

public class RPCUberService extends UberRideServiceGrpc.UberRideServiceImplBase {

    static final Logger log = LogManager.getLogger();

    private final ShardServer server;

    public RPCUberService(ShardServer server) {
        this.server = server;
    }

    @Override
    public void addRide(Ride request, StreamObserver<ID> responseObserver) {
        //UUID rideID = utils.UUID.generate();
        System.out.println("Requested Ride: " + request);
        responseObserver.onNext(ID.newBuilder().build());
        responseObserver.onCompleted();
    }
}
