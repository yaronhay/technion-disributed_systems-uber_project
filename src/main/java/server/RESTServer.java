package server;


import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import uber.proto.objects.*;
import uber.proto.objects.Date;
import uber.proto.rpc.PlanPathRequest;
import uber.proto.rpc.UberSnapshotRequest;
import utils.JSONConverters;
import utils.Utils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.Function;

public final class RESTServer extends utils.RESTController {
    static final Logger log = LogManager.getLogger();

    final ShardServer shardServer;

    public RESTServer(ShardServer shardServer, int port, Executor executor) throws IOException {
        super(port, executor);
        this.shardServer = shardServer;
    }

    Ride asRide(JSONObject req) {
        try {
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
                    srcID = shardServer.cityID.get(src);
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
                    dstID = shardServer.cityID.get(dst);
                }
                destination.setId(utils.UUID.toID(dstID));
            }
            return Ride.newBuilder()
                    .setDate(date)
                    .setSource(source.build())
                    .setDestination(destination.build())
                    .setProvider(provider)
                    .setPermittedDeviation(req.getFloat("permitted-deviation"))
                    .setVacancies(req.getInt("vacancies"))
                    .build();
        } catch (org.json.JSONException e) {
            log.error("Error on parsing ride JSON format", e);
            return null;
        }
    }

    @RestAPI(Context = "/rides_gossip", Method = "PUT")
    public void addRideGossip(JSONObject req, Response resp) {
        Ride ride = asRide(req);
        if (ride == null) {
            resp.httpCode = 400;
            resp.body.put("result", "Invalid Ride Information");
            return;
        }
        UUID cityID = utils.UUID.fromID(ride.getSource().getId());
        var cityShard = shardServer.cityShard.get(cityID);
        var shardServers = shardServer.shardsServers.get(cityShard);

        var serverID = utils.Random.getRandomKey(shardServers);
        var stub = shardServer.rpcClient.getServiceServerStub(cityShard, serverID);

        log.info("Submitting a new ride to server {} in shard {} with:\n{}", serverID, cityShard, req.toString(2));
        var res = stub.addRideGossip(ride); // Todo failure

        if (!res.getVal().isEmpty()) {
            UUID rideID = utils.UUID.fromID(res);
            resp.httpCode = 200;
            resp.body.put("result", "success");
            resp.body.put("ride-id", rideID.toString());
            log.info("Server {} in shard {} added the new ride successfully with id {}",
                    serverID, cityShard, rideID);
        } else {
            resp.httpCode = 500;
            resp.body.put("result", "failure");
            log.info("Server {} in shard {} failed to added the new ride successfully a failure response was sent to the client",
                    serverID, cityShard);
        }
    }

    @RestAPI(Context = "/rides", Method = "POST")
    public void addRide(JSONObject req, Response resp) {
        Ride ride = asRide(req);
        if (ride == null) {
            resp.httpCode = 400;
            resp.body.put("result", "Invalid Ride Information");
            return;
        }
        UUID cityID = utils.UUID.fromID(ride.getSource().getId());


        ID res = null;
        while (true) {
            var cityShard = shardServer.cityShard.get(cityID);
            var shardServers = shardServer.shardsServers.get(cityShard);
            if (shardServers.size() == 0) {
                break;
            }

            var serverID = utils.Random.getRandomKey(shardServers);
            log.info("Submitting a new ride to server {} in shard {} with:\n{}", serverID, cityShard, req.toString(2));
            var stub = shardServer.rpcClient.getServiceServerStub(cityShard, serverID);
            try {
                res = stub.addRide(ride);
            } catch (io.grpc.StatusRuntimeException e) {
                log.warn("Add ride StatusRuntimeException", e);
                continue;
            }

            if (res != null && !res.getVal().isEmpty()) {
                UUID rideID = utils.UUID.fromID(res);
                resp.httpCode = 200;
                resp.body.put("result", "success");
                resp.body.put("ride-id", rideID.toString());
                log.info("Server {} in shard {} added the new ride successfully with id {}",
                        serverID, cityShard, rideID);
                return;
            } else {
                resp.httpCode = 500;
                resp.body.put("result", "failure");
                log.info("Server {} in shard {} failed to didn't add the new ride successfully a failure response was sent to the client",
                        serverID, cityShard);
            }
        }
        resp.httpCode = 500;
        resp.body.put("result", "failure");
    }

    PlanPathRequest asPathRequest(JSONObject req, ID transactionID) {
        try {
            Date date = Date.newBuilder()
                    .setDay(req.getInt("day"))
                    .setMonth(req.getInt("month"))
                    .setYear(req.getInt("year"))
                    .build();
            User consumer = User.newBuilder()
                    .setFirstName(req.getString("firstname"))
                    .setLastName(req.getString("lastname"))
                    .setPhoneNumber(req.getString("phonenumber"))
                    .build();


            List<Hop> hops = new LinkedList<>();
            Function<String, City> parseCity = city -> {
                UUID id;
                try {
                    id = UUID.fromString(city);

                } catch (IllegalArgumentException e) {
                    id = shardServer.cityID.get(city);
                }
                return City
                        .newBuilder()
                        .setId(utils.UUID.toID(id))
                        .setName(shardServer.cityName.get(id))
                        .build();
            };

            var cityList = req.getJSONArray("cities");
            for (int i = 0; i < cityList.length() - 1; i++) {
                var src = parseCity.apply(cityList.getString(i));
                var dst = parseCity.apply(cityList.getString(i + 1));
                hops.add(Hop
                        .newBuilder()
                        .setSrc(src)
                        .setDst(dst)
                        .build()
                );
            }

            return PlanPathRequest.newBuilder()
                    .setTransactionID(transactionID)
                    .setDate(date)
                    .setConsumer(consumer)
                    .addAllHops(hops)
                    .build();
        } catch (org.json.JSONException e) {
            log.error("Error on parsing ride JSON format", e);
            return null;
        }
    }


    @RestAPI(Context = "/path", Method = "POST")
    public void planPath(JSONObject req, Response resp) {
        UUID transactionID = utils.UUID.generate();
        PlanPathRequest path = asPathRequest(req, utils.UUID.toID(transactionID));
        if (path == null) {
            resp.httpCode = 400;
            resp.body.put("result", "Invalid Path Information");
            return;
        }
        log.info("Trying to plan a new ride path (Transaction id {}):\n{}", transactionID, req.toString(2));

        var stub = shardServer.rpcClient.getServiceServerStub(
                shardServer.shard, shardServer.id);

        var plan = stub.planPath(path);

        if (plan.getSuccess()) {
            resp.httpCode = 200;
            resp.body.put("result", "success");
            resp.body.put("system-transaction-id", transactionID.toString());
            resp.body.put("rides", JSONConverters.toJSON(plan.getRidesList()));
            log.info("Path planning was successful (Transaction id {})", transactionID);
        } else {
            resp.httpCode = 500;
            resp.body.put("result", "failure");
            resp.body.put("system-transaction-id", transactionID.toString());
            log.info("Path planning failed (Transaction id {})", transactionID);
        }
    }

    @RestAPI(Context = "/snapshot", Method = "GET", hasJSONRequest = false)
    public void snapshot(JSONObject req, Response resp) {
        JSONObject rides = new JSONObject();

        Map<UUID, PlanPathRequest> plans = new HashMap<>();
        Map<UUID, List<Reservation>> planRides = new HashMap<>();

        log.info("Starting a new snapshot request");

        try {
            shardServer
                    .rpcClient
                    .getServiceServerStub(shardServer.shard, shardServer.id)
                    .snapshot(UberSnapshotRequest.newBuilder().build())
                    .forEachRemaining(snapshotResponse -> {
                        if (snapshotResponse.hasRideStatus()) {
                            var rideStatus = snapshotResponse.getRideStatus();
                            var rideID = utils.UUID.fromID(rideStatus.getRide().getId());
                            rides.put(rideID.toString(), JSONConverters.toJSON(rideStatus));

                            var reservations = rideStatus.getReservationsMap();
                            for (var entry : reservations.entrySet()) {
                                var hop = entry.getKey();
                                var reservation = entry.getValue();
                                var transactionID = utils.UUID.fromID(reservation.getTransactionID());
                                var l = planRides.computeIfAbsent(transactionID, k -> new LinkedList<>());
                                Utils.ensureListSize(l, hop);
                                l.set(hop - 1, reservation.toBuilder().setRideID(
                                        utils.UUID.toID(rideID)
                                ).build());
                            }
                        } else if (snapshotResponse.hasPathPlan()) {
                            var path = snapshotResponse.getPathPlan();
                            var transactionID = utils.UUID.fromID(path.getTransactionID());
                            plans.put(transactionID, path);
                            var l = planRides.computeIfAbsent(transactionID, k -> new LinkedList<>());
                            Utils.ensureListSize(l, path.getHopsCount());
                        }
                    });
        } catch (StatusRuntimeException e) {
            log.warn("Caught Status Runtime Exception");
            resp.httpCode = 500;
            resp.body.put("result", "failure");
            log.info("Snapshot failed");
            return;
        }

        resp.httpCode = 200;
        resp.body.put("result", "success");
        resp.body.put("snapshot",
                (new JSONObject())
                        .put("rides", rides)
                        .put("path-plans", JSONConverters.toJSON(plans, planRides)));
        log.info("Snapshot sent");
    }

}
