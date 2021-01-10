package server;

import cfg.CONFIG;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import uber.proto.zk.Server;
import zookeeper.ZK;
import zookeeper.ZKConnection;
import zookeeper.ZKPath;


import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ShardServer {

    static final Logger log = LogManager.getLogger();


    final UUID id;
    final UUID shard;
    final Map<UUID, Server> shardServers;

    final ZKConnection zk;

    final ShardData data;
    final Map<Integer, Boolean> cities;


    public ShardServer(ZKConnection zkCon, UUID shardID) {
        this.id = utils.UUID.generate();
        this.shard = shardID;
        shardServers = new ConcurrentHashMap<>();

        this.zk = zkCon;

        this.cities = new ConcurrentHashMap<>();
        this.data = null;

    }

    private void registerInShard(CONFIG.Server cfg) throws InterruptedException, KeeperException {
        var servers_shard = ZK.Path("shard_" + shard.toString(), "servers");
        try {
            this.zk.createPersistentPath(servers_shard);
        } catch (KeeperException e) {
            log.error("Error upon creating shard root ZNode", e);
            throw e;
        }

        Watcher updateShardServers = e -> {
            if (e.getType() == Watcher.Event.EventType.NodeCreated) {
                var parent = ZKPath.fromStr(e.getPath());
                parent = parent.prefix(parent.length() - 2); // Remove one
                List<ZKPath> children = null;
                try {
                    children = zk.getChildren(parent);
                } catch (KeeperException | InterruptedException err) {
                    err.printStackTrace();
                    log.error("Exception on getting existing node's children", e);
                    return;
                }

                for (var child : children) {
                    addServerToShardMembershipIfNotExists(child);
                }

            } else if (e.getType() == Watcher.Event.EventType.NodeDeleted) {
                var path = ZKPath.fromStr(e.getPath());
                var strid = path.get(path.length() - 1);
                UUID id = UUID.fromString(strid);
                this.shardServers.remove(id);
                log.info("Removed server {} from shard", id);
            }
        };
        this.zk.addPersistentRecursiveWatch(servers_shard, updateShardServers);
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

    private void addServerToShardMembershipIfNotExists(ZKPath path) {
        var strid = path.get(path.length() - 1);
        UUID id = UUID.fromString(strid);

        Function<UUID, Server> computer = (x) -> {
            log.debug("Adding new server from path {}", path.str());
            Server s = null;
            try {
                byte[] data = this.zk.getData(path);
                s = Server.parseFrom(data);
            } catch (KeeperException e) {
                if (e.code() == KeeperException.Code.NONODE) {
                    log.error("Created node does not exist in Membership Updating (Watcher)", e);
                } else {
                    log.error("KeeperException in Membership Updating (Watcher)", e);
                    e.printStackTrace();
                }
                return null;
            } catch (InterruptedException e) {
                e.printStackTrace();
                log.error("InterruptedException in Membership Updating (Watcher)", e);
                return null;
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
                log.error("Server (ProtocolBuf object) failed to parse, shouldn't happen", e);
            }
            log.info("Added new server {} to shard, at {}:{}(grpc),{}(rest)",
                    id, s.getHost(), s.getPorts().getGrpc(), s.getPorts().getRest());
            return s;
        };


        this.shardServers.computeIfAbsent(id, computer);

    }

    public void initGRPCServer() {

    }

    public boolean initialize(CONFIG.Server cfg) throws InterruptedException {

        try {
            this.registerInShard(cfg);
        } catch (KeeperException e) {
            return false;
        }

        return true;
    }


}
