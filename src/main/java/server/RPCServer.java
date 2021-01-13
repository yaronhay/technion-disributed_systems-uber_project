package server;

import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.ShutdownService;

import java.io.IOException;

public class RPCServer {
    static final Logger log = LogManager.getLogger();

    final io.grpc.Server rpcServer;

    public RPCServer(int port, ShardServer shardServer) {
        this.rpcServer = ServerBuilder
                .forPort(port)
                .addService(new RPCUberService(shardServer))
                .addService(new RPCShardCommunicationService(shardServer))
                .build();
        log.info("Adding GRPC Server at port {}", port);
    }

    public boolean start() {
        try {
            this.rpcServer.start();
            log.info("gRPC server started successfully");
        } catch (IOException e) {
            log.error("gRPC failed to start and ended with an error", e);
            return false;
        }
        ShutdownService.addHook(RPCServer.this::shutdown);
        return true;
    }

    public void shutdown() {
        try {
            this.rpcServer.shutdown().awaitTermination();
            log.info("gRPC service was shutdown");
        } catch (InterruptedException e) {
            log.error("gRPC service shutdown was interrupted", e);
        }
    }
}
