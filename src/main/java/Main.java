import cfg.CONFIG;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.ShardServer;
import utils.Host;
import zookeeper.ZKConnection;

import java.awt.*;
import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;


public class Main {
    static Logger log = LogManager.getLogger();


    static ZKConnection initZKConnection() throws IOException {
        var hosts = CONFIG.zkHosts;
        log.info("Connecting ZooKeeper Servers with host list: {}", Host.hostList(hosts));

        ZKConnection zk = new ZKConnection(hosts);
        // zk.connectedSync();
        return zk;
    }

    public static void start(CONFIG.Server server) {
        ZKConnection zk = null;
        try {
            zk = initZKConnection();
        } catch (IOException e) {
            zk.close();
            log.error("Cannot connect to ZooKeeper server", e);
            System.exit(1);
        }

        if (!zk.connectedSync()) {
            log.error("Failed to connect to ZooKeeper, aborting");
            System.exit(1);
        }

        var shardServer = new ShardServer(zk, new UUID(0, (long) server.shard));
        boolean stat = false;
        try {
            stat = shardServer.initialize(server);
        } catch (InterruptedException e) {
            log.error("Interrupted Execption on server initialization", e);
        }

        if (!stat) {
            log.error("Failed to init to shard server, aborting");
            System.exit(1);
        }
    }

    public static void main(String[] args) {

        int server_i = -1;
       /* try {
            server_i = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            log.error("Invalid server idx", e);
            System.exit(1);
        }*/
        var in = new Scanner(System.in);
        System.out.println("Insert ID: ");
        server_i =in.nextInt();

        CONFIG.Server server = null;
        try {
            server = CONFIG.servers.get(server_i);
        } catch (ArrayIndexOutOfBoundsException e) {
            log.error("Invalid server idx", e);
            System.exit(1);
        }

        start(server);
        while (true) {}
    }
}