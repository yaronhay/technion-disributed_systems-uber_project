package server;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uber.proto.rpc.OfferRidesRequest;
import uber.proto.rpc.OfferRidesResponse;
import uber.proto.rpc.ServerCommunicationGrpc;

import java.util.concurrent.ConcurrentHashMap;

public class ServerCommunicationService extends ServerCommunicationGrpc.ServerCommunicationImplBase {

    static final Logger log = LogManager.getLogger();

    private final ShardServer server;

    public ServerCommunicationService(ShardServer server) {
        this.server = server;
    }

    @Override public void offerRides(OfferRidesRequest request, StreamObserver<OfferRidesResponse> responseObserver) {
        super.offerRides(request, responseObserver);
    }
}
