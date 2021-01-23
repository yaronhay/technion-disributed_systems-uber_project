package server;

import cfg.CONFIG;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.javatuples.Pair;
import uber.proto.objects.*;
import uber.proto.rpc.PlanPathRequest;
import uber.proto.rpc.SnapshotRequest;
import uber.proto.rpc.SnapshotResponse;
import uber.proto.zk.*;
import uber.proto.zk.Shard;
import utils.ShutdownService;
import zookeeper.ZK;
import zookeeper.ZKConnection;
import zookeeper.ZKPath;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ShardServer {

    static final Logger log = LogManager.getLogger();

    final ServersWatcher serversWatcher;
    final QueueProcessor queueProcessor;

    public final Map<UUID, RPCUberService.SnapshotInfo> snapshotInfo;

    final UUID id;
    final UUID shard;

    final Map<UUID, Map<UUID, Server>> shardsServers; // Shard-ID -> { Server-ID -> Server }
    final Map<UUID, Map<UUID, String>> shardsCities;  // Shard-ID -> { City-ID -> City-Name }
    final Map<UUID, UUID> cityShard;  // City-ID -> Shard-ID
    final Map<String, UUID> cityID; // City-Name -> City-ID
    final Map<UUID, String> cityName; // City-ID -> City-Name
    final Map<UUID, City.Location> cityLoc; // City-ID -> City-Name

    final ZKConnection zk;
    RPCServer rpcServer;
    RPCClient rpcClient;
    RESTServer restServer;

    final ShardData data;

    final Executor executor;

    final ZKPath shardRoot;

    public ShardServer(ZKConnection zkCon, UUID shardID, Executor executor) {
        this.executor = executor;
        this.id = utils.UUID.generate();
        log.info("\nThis server \nID : {} \nShard ID {}", this.id, shardID);
        this.shard = shardID;
        shardsServers = new ConcurrentHashMap<>();
        shardsCities = new ConcurrentHashMap<>();

        serversWatcher = new ServersWatcher(this, executor);

        this.zk = zkCon;

        this.data = new ShardData(this);


        cityShard = new ConcurrentHashMap<>();
        cityID = new ConcurrentHashMap<>();
        cityName = new ConcurrentHashMap<>();
        cityLoc = new ConcurrentHashMap<>();

        shardRoot = ZK.Path("shards", shardID.toString());
        queueProcessor = new QueueProcessor(this, zk);
        snapshotInfo = new ConcurrentHashMap<>();
    }

    public Map<UUID, Server> serversInShard() {
        return this.shardsServers.get(this.shard);
    }

    private ZKPath registerMyShard(List<City> cities) throws KeeperException, InterruptedException {
        var path = ZK.Path("shards");
        try {
            this.zk.createPersistentPath(path);
        } catch (KeeperException e) {
            log.error("Error upon creating shard root ZNode", e);
            throw e;
        }

        var shardDataBuilder = Shard.newBuilder()
                .setId(utils.UUID.toByteString(this.shard));
        for (var city : cities) {
            shardDataBuilder.addCities(city);
        }

        path = path.append(shard.toString());
        try {
            var node = this.zk.createNode(path,
                    CreateMode.PERSISTENT,
                    shardDataBuilder.build().toByteArray());
            log.info("Shard {} znode was created", this.shard.toString());
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw e;
            }
        }

        try {
            var locks = path.append("locks");
            this.zk.createNode(locks,
                    CreateMode.PERSISTENT);
            log.info("Shard {}/locks znode was created", this.shard.toString());
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw e;
            }
        }

        try {
            var locks = path.append("queue");
            this.zk.createNode(locks,
                    CreateMode.PERSISTENT);
            log.info("Shard {}/queue znode was created", this.shard.toString());
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw e;
            }
        }

        return path;
    }


    private void registerInShard(CONFIG.Server cfg, List<City> shardCities) throws InterruptedException, KeeperException {
        var servers_shard = this
                .registerMyShard(shardCities)
                .append("servers");
        try {
            this.zk.createPersistentPath(servers_shard);
        } catch (KeeperException e) {
            log.error("Error upon creating shard root ZNode", e);
            throw e;
        }

        var server = Server.newBuilder()
                .setId(ByteString.copyFrom(utils.UUID.toBytes(this.id)))
                .setHost(cfg.host)
                .setPorts(Server.Ports.newBuilder()
                        .setGrpc(cfg.grpcPort)
                        .setRest(cfg.restPort)
                        .build()
                ).build();

        try {
            var server_znode = servers_shard.append(this.id.toString());
            ZKPath createdPath = this.zk.createNode(
                    server_znode,
                    CreateMode.EPHEMERAL,
                    server.toByteArray());
            assert server_znode.str().equals(createdPath.str());
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                throw new IllegalStateException("Unique server id already exists");
            } else {
                log.error("Error upon creating server znode id=" + this.id, e);
                throw e;
            }
        }
    }


    public boolean initGRPCServer(int port) {
        this.rpcServer = new RPCServer(port, this, executor);

        if (!this.rpcServer.start()) {
            return false;
        }
        ShutdownService.addHook(this.rpcServer::shutdown, "GRPC server");

        this.rpcClient = new RPCClient(this, executor);
        return true;
    }

    public boolean initRESTServer(int port) {
        try {
            log.info("Adding REST Server at port {}", port);
            this.restServer = new RESTServer(this, port, executor);
            this.restServer.start();
            ShutdownService.addHook(this.restServer::close, "REST Server");
            log.info("REST server started successfully");
            return true;
        } catch (IOException e) {
            log.error("Init REST server : IOException", e);
            return false;
        }
    }

    public boolean initialize(CONFIG.Server cfg, List<City> shardCities) throws InterruptedException {
        try {
            this.serversWatcher.initialize();
            this.registerInShard(cfg, shardCities);
            this.queueProcessor.initialize();
        } catch (KeeperException e) {
            return false;
        }

        if (!initRESTServer(cfg.restPort)) {
            return false;
        }

        if (!initGRPCServer(cfg.grpcPort)) {
            return false;
        }


        return true;
    }

    City getCityByName(String name) {
        var id = this.cityID.get(name);
        return City.newBuilder()
                .setId(utils.UUID.toID(id))
                .setName(name)
                .build();
    }
    City getCityByID(ID id) {
        var uuid = utils.UUID.fromID(id);
        var name = this.cityName.get(uuid);
        return City.newBuilder()
                .setId(id)
                .setName(name)
                .build();
    }

    public Map<UUID, UUID> getRandomServersForAllShards() {
        Map<UUID, UUID> servers = new HashMap<>();
        for (var k : this.shardsServers.entrySet()) {
            var shardID = k.getKey();
            var serverID = utils.Random.getRandomKey(k.getValue());
            if (serverID != null) {
                servers.put(serverID, shardID);
            }
        }
        return servers;
    }

    String tryLockSeat(UUID ride_id, int seat_no, UUID transactionID) throws InterruptedException, KeeperException {
        var lock = getSeatLockZNode(ride_id, seat_no);

        try {
            this.zk.createNode(lock, CreateMode.PERSISTENT);
            log.debug("Lock for seat {} of ride {} is created (Transaction ID {})", seat_no, ride_id, transactionID);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw e;
            }
        }


        var mylockpath = lock.append("lock_");
        mylockpath = this.zk.createNode(mylockpath, CreateMode.EPHEMERAL_SEQUENTIAL);
        var mylock = mylockpath.get(mylockpath.length() - 1);
        log.debug("Lock {} for seat {} of ride {} is created (Transaction ID {})", mylock, seat_no, ride_id, transactionID);

        var children = this.zk.getChildrenStr(lock);

        var min = Collections.min(children);
        log.debug("My lock is {} all children of {} are (min is {}) {}", mylock, lock.str(), min, children);
        if (min.equals(mylock)) {
            if (!this.zk.nodeExists(lock.append("final"))) {
                log.info("Has lock {} for seat {} in ride {} (Transaction ID {})", mylock, seat_no, ride_id, transactionID);
                // Has lock :)
                return mylock;
            } else {
                log.debug("Has lock {} for seat {} in ride {} for an invalidated lock, aborting (Transaction ID {})", min, seat_no, ride_id, transactionID);
            }
        }
        // Release lock
        this.releaseLockSeat(ride_id, seat_no, mylock,
                String.format("Release due to server failure acquire lock (Transaction ID %s)", transactionID));
        return null;
    }

    ZKPath getSeatLockZNode(UUID ride_id, int seat_no) {
        return this.shardRoot.append("locks", String.format("%s_%d", ride_id, seat_no));
    }

    ZKPath getShardQueueTaskZNode(UUID shardID) {
        return ZK.Path("shards", shardID.toString(), "queue", "op_");
    }

    void releaseLockSeat(UUID ride_id, int seat_no, String lock, String msg) throws InterruptedException, KeeperException {
        var seatLock = getSeatLockZNode(ride_id, seat_no);
        var mylockpath = seatLock.append(lock);

        try {
            if (this.zk.nodeExists(mylockpath)) {
                this.zk.delete(mylockpath);
            }
            log.info("Lock {} for seat {} of ride {} was released ({})", lock, seat_no, ride_id, msg);
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
                log.debug("Lock {} for seat {} of ride {} doesn't exist ({})", lock, seat_no, ride_id, msg);
            } else {
                throw e;
            }
        }

        try {
            if (this.zk.nodeExists(seatLock) && this.zk.getChildren(seatLock).isEmpty()) {
                this.zk.delete(seatLock);
            }
            log.debug("Lock for seat {} of ride {} was released", seat_no, ride_id);
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
                log.debug("Lock for seat {} of ride {} doesn't exist", seat_no, ride_id);
            } else if (e.code() == KeeperException.Code.NOTEMPTY) {
                log.debug("Lock for seat {} of ride {} is in use", seat_no, ride_id);
            } else {
                throw e;
            }
        }
    }


    public void invalidateSeatLock(UUID ride_id, int seat_no) throws InterruptedException, KeeperException {
        var seatLock = getSeatLockZNode(ride_id, seat_no);
        var seatLockFinal = seatLock.append("final");
        var seatLockMyFinal = seatLockFinal.append(this.id.toString());

        try {
            if (this.zk.nodeExists(seatLockFinal)) {
                this.zk.createRegularNode(seatLockMyFinal);
            }
            log.debug("Server invalidated the lock for seat {} of ride {}", seat_no, ride_id);
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
                log.debug("Lock for seat {} of ride {} doesn't exist", seat_no, ride_id);
            } else {
                throw e;
            }
        }

        try {
            var serversThatInvalidated = this.zk
                    .getChildrenStr(seatLockFinal)
                    .stream()
                    .map(UUID::fromString)
                    .collect(Collectors.toSet());

            var servers = this.serversInShard().keySet();

            if (serversThatInvalidated.containsAll(servers)) {
                this.zk.deleteSubTree(seatLock);
            }
            log.info("Lock for seat {} of ride {} was removed permanently", seat_no, ride_id);
        } catch (KeeperException e) {
            log.error("Keeper exception thrown while permanently the lock for seat {} in ride {}:\n{}", seat_no, ride_id, e);
        }
    }

    public void invalidateSeatLockAsync(UUID ride_id, int seat_no) throws InterruptedException, KeeperException {
        var seatLock = getSeatLockZNode(ride_id, seat_no);
        var seatLockFinal = seatLock.append("final");
        var seatLockMyFinal = seatLock.append(this.id.toString());

        try {
            if (this.zk.nodeExists(seatLockFinal)) {
                this.zk.createRegularNode(seatLockMyFinal);
            }
            log.debug("Server invalidated the lock for seat {} of ride {}", seat_no, ride_id);
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
                log.debug("Lock for seat {} of ride {} doesn't exist", seat_no, ride_id);
            } else {
                throw e;
            }
        }

        try {
            var serversThatInvalidated = this.zk
                    .getChildrenStr(seatLock)
                    .stream()
                    .map(UUID::fromString)
                    .collect(Collectors.toSet());

            var servers = this.serversInShard().keySet();

            if (serversThatInvalidated.containsAll(servers)) {
                this.zk.deleteSubTree(seatLock);
            }
            log.info("Lock for seat {} of ride {} was removed permanently", seat_no, ride_id);
        } catch (KeeperException e) {
            log.error("Keeper exception thrown while permanently the lock for seat {} in ride {}:\n{}", seat_no, ride_id, e);
        }
    }

    boolean atomicSeatsReserve(AtomicReferenceArray<RPCUberService.OfferCollector.Offer> offers,
                               User consumer, UUID transactionID, PlanPathRequest request) {
        log.debug("Starting atomic seats reservation (Transaction ID {})", transactionID);
        List<org.apache.zookeeper.Op> ops = new LinkedList<>();
        Map<UUID, List<Task>> shardTasks = new HashMap<>();

        UUID pathSaveShardID = this.cityShard.get(
                utils.UUID.fromID(request.getHops(0).getSrc().getId())
        );
        shardTasks.computeIfAbsent(pathSaveShardID, k -> new LinkedList<>())
                .add(Task.newBuilder()
                        .setAddPath(
                                AddPathTask.newBuilder()
                                        .setPath(request)
                                        .build()
                        )
                        .build());

        for (int i = 0; i < offers.length(); i++) {
            var offer = offers.get(i);

            var shardID = offer.shardID;
            var rideID = offer.rideOffer.getRideID();
            var rideUUID = utils.UUID.fromID(rideID);
            var seat = offer.rideOffer.getSeat();

            var seatLockZNode = getSeatLockZNode(rideUUID, seat);

            var finalLockZnode = seatLockZNode.append("final");
            ops.add(ZK.Op.createNode(finalLockZnode, CreateMode.PERSISTENT));
            log.debug("Atomic seats reservation (Transaction ID {}) - Adding invalidation for lock {}#{}",
                    transactionID, rideUUID, seat);


            var tasks = shardTasks.computeIfAbsent(shardID, k -> new LinkedList<>());

            tasks.add(Task
                    .newBuilder()
                    .setInvalidSeatLock(InvalidSeatLockTask.newBuilder()
                            .setRideID(rideID)
                            .setSeat(seat)
                            .build())
                    .build()
            );
            log.debug("Atomic seats reservation (Transaction ID {}) - Adding invalidation task for lock {}#{} to shard {}",
                    transactionID, rideUUID, seat, shardID);

            tasks.add(Task
                    .newBuilder()
                    .setReserve(ReserveTask.newBuilder()
                            .setRideID(rideID)
                            .setSeat(seat)
                            .setSource(offer.rideOffer.getRideInfo().getSource())
                            .setReservation(Reservation.newBuilder()
                                    .setConsumer(consumer)
                                    .setTransactionID(utils.UUID.toID(transactionID))
                                    .build())
                            .build())
                    .build()
            );
            log.debug("Atomic seats reservation (Transaction ID {}) - Adding reservation task for seat {}#{} for User({}, {}, {}) to shard {}",
                    transactionID, rideUUID, seat, consumer.getFirstName(), consumer.getLastName(), consumer.getPhoneNumber(), shardID);
        }


        var shardsTaskLists = shardTasks
                .entrySet()
                .stream()
                .map(e -> Pair.with(
                        e.getKey(),
                        TaskList.newBuilder().addAllTaskList(e.getValue()).build()
                        )
                )
                .collect(Collectors.toMap(Pair::getValue0, Pair::getValue1));

        for (var entry : shardsTaskLists.entrySet()) {
            var shardID = entry.getKey();
            var taskList = entry.getValue();

            ops.add(ZK.Op.createNode(
                    getShardQueueTaskZNode(shardID),
                    CreateMode.PERSISTENT_SEQUENTIAL,
                    taskList.toByteArray()));

        }

        log.debug("Submitting atomic seats reservation (Transaction ID {})", transactionID);
        List<OpResult> results;
        try {
            results = this.zk.atomic(ops);

        } catch (KeeperException e) {
            List<OpResult> resultserr = e.getResults();
            log.error(new ParameterizedMessage("KeeperException during the atomic seats reservation (Transaction ID {}),\nException {} for path {}\nResults:\n\t{}",
                    transactionID, e.getMessage(), e.getPath(),
                    IntStream.range(0, resultserr.size())
                            .mapToObj(i -> resultserr.get(i).toString() + " : " + ops.get(i).toString())
                            .collect(Collectors.joining("\n\t"))), e);
            return false;
        } catch (InterruptedException e) {
            log.error("InterruptedException during the atomic seats reservation (Transaction ID {})", transactionID, e);
            return false;
        }
        log.debug("Atomic seats reservation (Transaction ID {}) finished successfully\nResults:\n\t{}",
                transactionID, IntStream.range(0, results.size())
                        .mapToObj(i -> results.get(i).toString() + " : " + ops.get(i).toString())
                        .collect(Collectors.joining("\n\t")));
        return true;
    }
    public boolean startSnapshotTask(UUID snapshotID, Map<UUID, UUID> servers) {
        log.debug("Starting atomic snapshot task (Snapshot ID {})", snapshotID);
        List<org.apache.zookeeper.Op> ops = new LinkedList<>();

        for (var entry : servers.entrySet()) {
            var serverID = entry.getKey();
            var shardID = entry.getValue();

            var snapshotTask = SnapshotTask.newBuilder()
                    .setSnapshotID(utils.UUID.toID(snapshotID))
                    .setRequestedServer(SnapshotTask.ServerEndPoint
                            .newBuilder()
                            .setShardID(utils.UUID.toID(shardID))
                            .setServerID(utils.UUID.toID(serverID))
                            .build())
                    .setSendTo(SnapshotTask.ServerEndPoint
                            .newBuilder()
                            .setShardID(utils.UUID.toID(this.shard))
                            .setServerID(utils.UUID.toID(this.id))
                            .build())
                    .build();
            log.info("Adding snapshot task for shard {} (server {})", shardID, serverID);

            ops.add(ZK.Op.createNode(
                    getShardQueueTaskZNode(shardID),
                    CreateMode.PERSISTENT_SEQUENTIAL,
                    TaskList.newBuilder()
                            .addTaskList(Task.newBuilder()
                                    .setSnapshot(snapshotTask)
                                    .build()
                            )
                            .build()
                            .toByteArray()
            ));
        }
        log.debug("Submitting atomic snapshot task (Snapshot ID {})", snapshotID);
        try {
            this.zk.atomic(ops);
        } catch (KeeperException e) {
            log.error("KeeperException during the atomic snapshot task (Snapshot ID {})", snapshotID);
            return false;
        } catch (InterruptedException e) {
            log.error("InterruptedException during the atomic snapshot task (Snapshot ID {})", snapshotID);
            return false;
        }
        log.debug("Atomic snapshot task (Snapshot ID {}) submitted successfully", snapshotID);
        return true;
    }

    public void sendSnapshot(UUID snapshotID, UUID toShardID, UUID toServerID) {
        var streamObserver = this.
                rpcClient
                .getServerStub(toShardID, toServerID)
                .sendSnapshot(new StreamObserver<SnapshotResponse>() {
                    @Override public void onNext(SnapshotResponse snapshotResponse) { }
                    @Override public void onError(Throwable throwable) { }
                    @Override public void onCompleted() { }
                });
        streamObserver.onNext(SnapshotRequest.newBuilder()
                .setSnapshotID(utils.UUID.toID(snapshotID))
                .build());
        this.data.sendSnapshot(streamObserver);
        streamObserver.onCompleted();
    }

    public boolean atomicAddRide(UUID rideID, Ride ride) {
        log.debug("Starting atomic add ride task (Ride ID {})", rideID);
        List<org.apache.zookeeper.Op> ops = new LinkedList<>();

        var addRideTask = AddRideTask.newBuilder().setRide(ride).build();
        var task = Task.newBuilder().setAddRide(addRideTask).build();
        var data = TaskList.newBuilder().addTaskList(task).build().toByteArray();

        log.debug("Submitting atomic add ride task (Ride ID {})", rideID);
        try {
            this.zk.createNode(getShardQueueTaskZNode(this.shard),
                    CreateMode.PERSISTENT_SEQUENTIAL,
                    data);
        } catch (KeeperException e) {
            log.error("KeeperException during the atomic add ride task (Ride ID {})", rideID);
            return false;
        } catch (InterruptedException e) {
            log.error("InterruptedException during the atomic add ride task (Ride ID {})", rideID);
            return false;
        }
        log.debug("Atomic add ride task (Ride ID {}) submitted successfully", rideID);
        return true;
    }
}

