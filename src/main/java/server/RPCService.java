package server;

import io.grpc.stub.StreamObserver;
import uber.proto.objects.ID;
import uber.proto.objects.Ride;
import uber.proto.rpc.UberRPCServiceGrpc;

public class RPCService extends UberRPCServiceGrpc.UberRPCServiceImplBase {
    private final ShardServer server;

    public RPCService(ShardServer server) {
        this.server = server;
    }

    @Override
    public void addRide(Ride request, StreamObserver<ID> responseObserver) {
        super.addRide(request, responseObserver);
    }
}
