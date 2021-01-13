package server;


import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import uber.proto.objects.*;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public final class RESTServer extends utils.RESTController {
    static final Logger log = LogManager.getLogger();

    final ShardServer shardServer;

    public RESTServer(ShardServer shardServer, int port) throws IOException {
        super(port);
        this.shardServer = shardServer;
    }


    @RestAPI(Context = "/hello", Method = "POST")
    public void sayHello(JSONObject obj, Response resp) {
        System.out.println(obj.toString());
        resp.httpCode = 200;
        resp.body.put("hi", "hi");
    }

    Ride asRide(JSONObject req, UUID id) {
        try {
//            ID id1 = ID.newBuilder()
//                    .setVal(
//                            ByteString.copyFrom(utils.UUID.toBytes(id))
//                    )
//                    .build();

            Date date = Date.newBuilder()
                    .setDay(req.getInt("day"))
                    .setMonth(req.getInt("month"))
                    .setYear(req.getInt("year"))
                    .build();
            User provider = User.newBuilder()
                    .setFirstName(req.getString("firstname"))
                    .setLastName(req.getString("lastname"))
                    .setPhoneNumber(req.getString("phonenumber"))
                    .build();

            var source = City.newBuilder();
            {
                var src = req.getString("source");
                UUID srcID;
                try {
                    srcID = UUID.fromString(src);

                } catch (IllegalArgumentException e) {
                    srcID = shardServer.cityName.get(src);
                }
                source.setId(utils.UUID.toID(srcID));
            }

            var destination = City.newBuilder();
            {
                var dst = req.getString("destination");
                UUID dstID;
                try {
                    dstID = UUID.fromString(dst);

                } catch (IllegalArgumentException e) {
                    dstID = shardServer.cityName.get(dst);
                }
                destination.setId(utils.UUID.toID(dstID));
            }
            return Ride.newBuilder()
                    // .setId(id1)
                    .setDate(date)
                    .setSource(source.build())
                    .setDestination(destination.build())
                    .setProvider(provider)
                    .build();
        } catch (org.json.JSONException e) {
            log.error("Error on parsing ride JSON format", e);
            return null;
        }
    }
    @RestAPI(Context = "/rides", Method = "PUT")
    public void addRide(JSONObject req, Response resp) {
        // UUID rideID = utils.UUID.generate();
        Ride ride = asRide(req, null);
        if (ride == null) {
            resp.httpCode = 400;
            resp.body.put("result", "Invalid Ride Information");
            return;
        }
        UUID cityID = utils.UUID.fromID(ride.getSource().getId());
        var cityShard = shardServer.cityShard.get(cityID);
        var shardServers = shardServer.shardsServers.get(cityShard);

        var serverID = utils.Random.getRandomKey(shardServers);
        var stub = shardServer.rpcClient.getServerStub(cityShard, serverID);

        UUID rideID = null;

        var res = stub.addRide(ride);

        if (!res.getVal().isEmpty()) {
            rideID = utils.UUID.fromID(res);
            resp.httpCode = 200;
            resp.body.put("result", "success");
            resp.body.put("ride-id", rideID.toString());
        } else {
            resp.httpCode = 200;
            resp.body.put("result", "failure");
        }

    }

    public static void main(String[] args) throws IOException {
//        RESTServer restServer = new RESTServer(null, 5000);
//        restServer.start();
        System.out.println(ID.newBuilder().build().getVal().isEmpty());
    }
}
