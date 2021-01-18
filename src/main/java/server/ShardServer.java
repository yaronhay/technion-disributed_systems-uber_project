package server;

import cfg.CONFIG;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import uber.proto.objects.City;
import uber.proto.objects.ID;
import uber.proto.zk.Server;
import uber.proto.zk.Shard;
import zookeeper.ZK;
import zookeeper.ZKConnection;
import zookeeper.ZKPath;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class ShardServer {

    static final Logger log = LogManager.getLogger();


    final ServersWatcher serversWatcher;

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

        serversWatcher = new ServersWatcher(this);

        this.zk = zkCon;

        this.data = new ShardData(this);


        cityShard = new ConcurrentHashMap<>();
        cityID = new ConcurrentHashMap<>();
        cityName = new ConcurrentHashMap<>();
        cityLoc = new ConcurrentHashMap<>();

        shardRoot = ZK.Path("shards", shardID.toString());
    }

    public Map<UUID, Server> serversInShard() {
        return this.shardsServers.get(this.shard);
    }

    private ZKPath registerShard(List<City> cities) throws KeeperException, InterruptedException {
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


    private void registerInShard(CONFIG.Server cfg) throws InterruptedException, KeeperException {
        var servers_shard = this
                .registerShard(CONFIG.getShardCities(this.shard))
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

        this.rpcClient = new RPCClient(this, executor);
        return true;
    }

    public boolean initRESTServer(int port) {
        try {
            log.info("Adding REST Server at port {}", port);
            this.restServer = new RESTServer(this, port);
            this.restServer.start();
            log.info("REST server started successfully");
            return true;
        } catch (IOException e) {
            log.error("Init REST server : IOException", e);
            return false;
        }
    }

    public boolean initialize(CONFIG.Server cfg) throws InterruptedException {
        try {
            this.serversWatcher.initialize();
            this.registerInShard(cfg);
        } catch (KeeperException e) {
            return false;
        }

        if (!initGRPCServer(cfg.grpcPort)) {
            return false;
        }

        if (!initRESTServer(cfg.restPort)) {
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


    boolean tryLockSeat(UUID ride_id, int seat_no) throws InterruptedException, KeeperException {
        var lock = this.shardRoot.append("locks", String.format("%s_%d", ride_id, seat_no));

        try {
            this.zk.createNode(lock, CreateMode.PERSISTENT);
            log.debug("Lock for seat {} of ride {} is created", seat_no, ride_id);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw e;
            }
        }


        var mylockpath = lock.append("lock_");
        mylockpath = this.zk.createNode(mylockpath, CreateMode.EPHEMERAL_SEQUENTIAL);
        var mylock = mylockpath.get(mylockpath.length() - 1);
        log.debug("Lock for seat {} of ride {} is created", seat_no, ride_id);

        var children = this.zk.getChildrenStr(lock);
        var min = Collections.min(children);

        if (min.equals(mylock)) {
            log.debug("Has lock for seat {} of ride {} is created", seat_no, ride_id);
            // Has lock :)
            return true;
        } else {
            // Release lock
            this.zk.delete(mylockpath);
            return false;
        }
    }

}
