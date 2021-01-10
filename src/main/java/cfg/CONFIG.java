package cfg;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.Host;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class CONFIG {
    static Logger log = LogManager.getLogger();

    public final static List<Host> zkHosts;

    static {
        zkHosts = new ArrayList<>();
        try {

            zkHosts.add(new Host("127.0.0.1", 2181));

        } catch (UnknownHostException e) {
            log.error("Error on parsing ZK host address", e);
            System.exit(1);
        }
    }

    public static class Server {
        public final String host;
        public final int shard;
        public final int grpcPort;
        public final int restPort;

        public Server(String host, int shard, int grpcPort, int restPort) {
            this.host = host;
            this.shard = shard;
            this.grpcPort = grpcPort;
            this.restPort = restPort;
        }
    }

    // Index is city id
    public final static String[] cities = {
            "A",
            "B",
            "C"
    };

    // Index is server id
    public final static Server[] servers = {
            new Server("localhost", 1, 1000, 1000),
            new Server("localhost", 1, 1000, 1000),
            new Server("localhost", 1, 1000, 1000),
            new Server("localhost", 1, 1000, 1000),
            new Server("localhost", 1, 1000, 1000),
    };

    // Index is shard id
    public final static int[][] shards = {
            {},
            {}
    };

    // Index is shard id
    public final static int[][] shardServers;

    static {
        shardServers = new int[shards.length][];
    }

}
