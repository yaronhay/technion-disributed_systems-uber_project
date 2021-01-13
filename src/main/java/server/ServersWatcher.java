package server;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import uber.proto.zk.Server;
import uber.proto.zk.Shard;
import zookeeper.ZK;
import zookeeper.ZKPath;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ServersWatcher {
    static final Logger log = LogManager.getLogger();

    final ShardServer server;

    public ServersWatcher(ShardServer server) {
        this.server = server;
    }

    public void initialize() throws KeeperException, InterruptedException {
        this.watchShards();
    }

    void addShard(UUID shardID) {
        Function<UUID, Map<UUID, String>> computer = k -> {
            var path = ZK.Path("shards", shardID.toString());
            uber.proto.zk.Shard data = null;
            try {
                byte[] _data = server.zk.getData(path);
                data = Shard.parseFrom(_data);
            } catch (KeeperException e) {
                log.error("Error when pulling shard data", e);
            } catch (InvalidProtocolBufferException e) {
                log.error("Shard (ProtocolBuf object) failed to parse, shouldn't happen", e);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Map<UUID, String> cities = new ConcurrentHashMap<>();
            for (int i = 0; i < data.getCitiesCount(); i++) {
                var city = data.getCities(i);
                var id = utils.UUID.fromID(city.getId());
                var name = city.getName();
                cities.put(id, name);
                server.cityShard.putIfAbsent(id, shardID);
                server.cityName.putIfAbsent(name, id);
                log.info("Added City {} ({}) to shard {}",
                        name, id.toString(), shardID.toString());
            }
            return cities;
        };
        server.shardsCities.computeIfAbsent(shardID, computer);
    }
    void watchShards() throws InterruptedException, KeeperException {
        var path = ZK.Path("shards");
        try {
            server.zk.createPersistentPath(path);
        } catch (KeeperException e) {
            log.error("Error upon creating shard root ZNode", e);
            throw e;
        }

        List<ZKPath> children = null;
        try {
            children = server.zk.getChildren(path);
        } catch (KeeperException | InterruptedException err) {
            err.printStackTrace();
            log.error("Exception on getting existing shards list", err);
            throw err;
        }

        for (var child : children) {
            watchShard(child);
        }
        server.zk.addPersistentRecursiveWatch(path, this::shardsWatcher);
    }


    void shardsWatcher(WatchedEvent e) {
        if (e.getType() == Watcher.Event.EventType.NodeCreated) {
            var path = ZKPath.fromStr(e.getPath());
            if (path.length() != 2) {
                return;
            }
            watchShard(path);
        }

    }
    private void watchShard(ZKPath path) {
        UUID shardID = UUID.fromString(path.get(path.length() - 1));
        this.addShard(shardID);

        path = path.append("servers");
        try {
            server.zk.createPersistentPath(path);
        } catch (KeeperException | InterruptedException err) {
            log.error("Error upon creating shard root ZNode", err);
        }

        List<ZKPath> children = null;
        try {
            children = server.zk.getChildren(path);
        } catch (KeeperException | InterruptedException err) {
            err.printStackTrace();
            log.error("Exception on getting existing node's children", err);
            return;
        }

        for (var child : children) {
            addServerToShardMembership(shardID, child);
        }

        try {
            server.zk.addPersistentRecursiveWatch(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                    var epath = ZKPath.fromStr(event.getPath());
                    addServerToShardMembership(shardID, epath);

                } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    var epath = ZKPath.fromStr(event.getPath());
                    removeServerFromShardMembership(shardID, epath);
                }
            });
        } catch (KeeperException | InterruptedException err) {
            log.error("Exception when adding watch to shard " + shardID, err);
        }
    }
    void addServerToShardMembership(UUID shardID, ZKPath path) {
        UUID id = UUID.fromString(path.get(path.length() - 1));

        Function<UUID, Server> computer = (x) -> {
            log.debug("Adding new server from path {}", path.str());
            Server s = null;
            try {
                byte[] data = server.zk.getData(path);
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


        var shard = server.shardsServers
                .computeIfAbsent(shardID, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(id, computer);

    }

    void removeServerFromShardMembership(UUID shardID, ZKPath path) {
        UUID serverID = UUID.fromString(path.get(path.length() - 1));
        server.shardsServers.get(shardID).remove(serverID);
        log.info("Removed server {} from shard {}", serverID, shardID);
    }

}
