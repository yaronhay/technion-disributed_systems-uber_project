package server;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uber.proto.rpc.OfferRidesRequest;
import uber.proto.rpc.OfferRidesResponse;
import uber.proto.rpc.ServerCommunicationGrpc;

import java.util.concurrent.ConcurrentHashMap;

public class RPCServerCommunicationService extends ServerCommunicationGrpc.ServerCommunicationImplBase {

    static final Logger log = LogManager.getLogger();

    private final ShardServer server;

    public RPCServerCommunicationService(ShardServer server) {
        this.server = server;
    }

    @Override public void offerRides(OfferRidesRequest request, StreamObserver<OfferRidesResponse> responseObserver) {
        var transactionID = utils.UUID.fromID(request.getTransactionID());
        var date = request.getDate();
        var hops = request.getHopsList();

        var res = server.data.offerPath(date, hops, transactionID);
        var offers = res.getValue0();
        var seats = res.getValue1();

        var ridesResponse = OfferRidesResponse.newBuilder();
        for (int i = 0; i < offers.length; i++) {
            if (offers[i] != null) {
                var src = hops.get(i).getSrc();
                var dst = hops.get(i).getDst();
                ridesResponse.putOffers(i + 1,
                        OfferRidesResponse.RideOffer
                                .newBuilder()
                                .setRideID(utils.UUID.toID(offers[i]))
                                .setSource(src)
                                .setSeat(seats[i])
                                .setDestination(dst)
                                .build());
                var srcID = utils.UUID.fromID(src.getId());
                var dstID = utils.UUID.fromID(dst.getId());
                log.info("Adding offer ride {}_{} as (Transaction ID {}) for hop from {} to {} ",
                        offers[i], seats[i], transactionID, server.cityName.get(srcID), server.cityName.get(dstID));

            }
        }
        responseObserver.onNext(ridesResponse.build());
        responseObserver.onCompleted();
        log.info("Finished sending offers (Transaction ID {})", transactionID);
    }
}
