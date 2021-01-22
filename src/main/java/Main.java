import cfg.CONFIG;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.ShardServer;
import uber.proto.objects.City;
import utils.Host;
import zookeeper.ZKConnection;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


public class Main {
    static Logger log = LogManager.getLogger();


    static ZKConnection initZKConnection(List<Host> hosts) throws IOException {
        log.info("Connecting ZooKeeper Servers with host list: {}", Host.hostList(hosts));

        ZKConnection zk = new ZKConnection(hosts);
        return zk;
    }

    static List<Host> getHosts(String file) {
        List<Host> l = new LinkedList<>();
        Scanner s = null;
        try {
            s = new Scanner(new File(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
        while (s.hasNext()) {
            var host = s.next();
            var port = s.nextInt();
            try {
                l.add(new Host(host, port));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        s.close();
        return l;
    }

    public static void start(List<Host> zkHosts, CONFIG.Server server, List<City> shardCities) {
        Executor executor = Executors.newCachedThreadPool();
        ZKConnection zk = null;
        try {
            zk = initZKConnection(zkHosts);
        } catch (IOException e) {
            log.error("Cannot connect to ZooKeeper server", e);
            System.exit(1);
        }

        if (!zk.connectedSync()) {
            log.error("Failed to connect to ZooKeeper, aborting");
            System.exit(1);
        }

        var shardServer = new ShardServer(zk, new UUID(0, server.shard), executor);
        boolean stat = false;
        try {
            stat = shardServer.initialize(server, shardCities);
        } catch (InterruptedException e) {
            log.error("Interrupted Execution on server initialization", e);
        }

        if (!stat) {
            log.error("Failed to init to shard server, aborting");
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        String zkHost = args[0];
        var zkHosts = getHosts(zkHost);

        String shardfile = args[1];
        long shardID = 0;
        {
            var s = FilenameUtils.getBaseName(shardfile);
            s = FilenameUtils.removeExtension(s);
            shardID = Long.parseLong(s);
        }
        List<City> shardCities = getCities(shardfile);

        String thisAdd = args[2];
        CONFIG.Server server;

        {
            var split = thisAdd.split(":");
            var host = split[0];
            var grpc = Integer.parseInt(split[1]);
            var rest = Integer.parseInt(split[2]);
            server = new CONFIG.Server(host,shardID,grpc, rest);
        }

        start(zkHosts, server, shardCities);
    }
    private static List<City> getCities(String file) {
        List<City> l = new LinkedList<>();
        Scanner s = null;
        try {
            s = new Scanner(new File(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
        while (s.hasNext()) {
            var name = s.next();
            var x = s.nextInt();
            var y = s.nextInt();

            l.add(City.newBuilder()
                    .setName(name)
                    .setLocation(City.Location
                            .newBuilder()
                            .setX(x)
                            .setY(y)
                            .build())
                    .setId(utils.UUID.toID(utils.UUID.generate()))
                    .build());
        }
        s.close();
        return l;
    }
}