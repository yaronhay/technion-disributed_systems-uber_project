package server;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uber.proto.rpc.UberRideServiceGrpc;
import uber.proto.rpc.UberShardCommunicationGrpc;
import utils.Host;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class RPCClient {
    static final Logger log = LogManager.getLogger();

    final ShardServer server;

    final Map<String, Channel> channels;
    final Map<UUID, UberRideServiceGrpc.UberRideServiceBlockingStub> serversRPCStubs;
    final Map<UUID, UberShardCommunicationGrpc.UberShardCommunicationStub> shardRPCStubs;

    public RPCClient(ShardServer server) {
        this.server = server;
        this.serversRPCStubs = new ConcurrentHashMap<>();
        this.channels = new ConcurrentHashMap<>();
        this.shardRPCStubs = new ConcurrentHashMap<>();
    }

    public UberRideServiceGrpc.UberRideServiceBlockingStub getServerStub(UUID shardID, UUID serverID) {
        Function<UUID, UberRideServiceGrpc.UberRideServiceBlockingStub> creator = id -> {
            var server = RPCClient.this
                    .server.shardsServers.get(shardID).get(serverID);
            if (server == null) {
                log.error("Server with id {} was not found", serverID.toString());
                return null;
            }

            Channel channel = getChannel(server);
            if (channel == null) return null;
            return UberRideServiceGrpc.newBlockingStub(channel);
        };
        return serversRPCStubs.computeIfAbsent(serverID, creator);
    }
    public UberShardCommunicationGrpc.UberShardCommunicationStub getShardServerStub(UUID serverID) {
        Function<UUID, UberShardCommunicationGrpc.UberShardCommunicationStub> creator = id -> {
            var server = RPCClient.this
                    .server.shardsServers.get(this.server.shard).get(serverID);
            if (server == null) {
                log.error("Shard Server with id {} was not found", serverID.toString());
                return null;
            }

            Channel channel = getChannel(server);
            if (channel == null) return null;
            return UberShardCommunicationGrpc.newStub(channel);
        };
        return shardRPCStubs.computeIfAbsent(serverID, creator);
    }

    private Channel getChannel(uber.proto.zk.Server server) {
        Host host = null;
        try {
            host = new Host(server.getHost(), server.getPorts().getGrpc());
        } catch (UnknownHostException e) {
            log.error("Shard server host is unknown", e);
            return null;
        }
        var channel = RPCClient.this.channels.computeIfAbsent(
                host.str(),
                h -> ManagedChannelBuilder
                        .forTarget(h)
                        .usePlaintext()
                        .build()
        );
        return channel;
    }
}
