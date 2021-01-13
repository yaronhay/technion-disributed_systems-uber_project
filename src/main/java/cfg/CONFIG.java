package cfg;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uber.proto.objects.City;
import uber.proto.objects.ID;
import utils.Host;
import utils.UUID;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
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
    public final static List<String> cities = new LinkedList<>();


    // Index is server id
    public final static List<Server> servers;

    // Index is shard id
    public final static List<List<Integer>> shards = new LinkedList<>();

    static {
        List<Server> list = new LinkedList<Server>();

        for (int i = 0; i < 5; i++) {
            var l = new LinkedList<Integer>();
            shards.add(l);
            for (int j = 0; j < 3; j++) {
                int serveridx = i * 5 + j;
                list.add(new Server("localhost", i, 5000 + serveridx, 6000 + serveridx));
                cities.add("city" + (serveridx + 1));
                l.add(serveridx);
            }
        }
        servers = list;
    }


    public static List<City> getShardCities(java.util.UUID id) {
        int idx = (int) id.getLeastSignificantBits();
        var shrd = shards.get(idx);
        List<City> ret = new ArrayList<>(shrd.size());

        for (var city : shrd) {
            ret.add(City.newBuilder()
                    .setName(cities.get(city))
                    .setId(UUID.toID(UUID.generate()))
                    .build());
        }

        return ret;
    }

}
