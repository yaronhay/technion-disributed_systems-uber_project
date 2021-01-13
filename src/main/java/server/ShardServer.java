package server;

import cfg.CONFIG;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import uber.proto.objects.City;
import uber.proto.zk.Server;
import uber.proto.zk.Shard;
import zookeeper.ZK;
import zookeeper.ZKConnection;
import zookeeper.ZKPath;


import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ShardServer {

    static final Logger log = LogManager.getLogger();


    final ServersWatcher serversWatcher;

    final UUID id;
    final UUID shard;
    final Map<UUID, Map<UUID, Server>> shardsServers;
    final Map<UUID, Map<UUID, String>> shardsCities;
    final Map<UUID, UUID> cityShard;
    final Map<String, UUID> cityName;

    final ZKConnection zk;
    RPCServer rpcServer;
    RPCClient rpcClient;
    RESTServer restServer;

    final ShardData data;


    public ShardServer(ZKConnection zkCon, UUID shardID) {
        this.id = utils.UUID.generate();
        this.shard = shardID;
        shardsServers = new ConcurrentHashMap<>();
        shardsCities = new ConcurrentHashMap<>();

        serversWatcher = new ServersWatcher(this);

        this.zk = zkCon;

        this.data = new ShardData();


        cityShard = new ConcurrentHashMap<>();
        cityName = new ConcurrentHashMap<>();
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
        this.rpcServer = new RPCServer(port, this);

        if (!this.rpcServer.start()) {
            return false;
        }

        this.rpcClient = new RPCClient(this);
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


}
