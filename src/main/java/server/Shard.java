package server;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import zookeeper.ZK;
import zookeeper.ZKConnection;
import zookeeper.ZKPath;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Shard {

    public static class Path {

        static class cache {
            static Map<String, ZKPath> newCache() { return new ConcurrentHashMap<>(); }

            static Map<String, ZKPath> root = newCache();
            static Map<String, ZKPath> servers = newCache();
        }

        public static ZKPath Root(String city) {
            return cache.root.computeIfAbsent(city, c -> ZK.Path("cities", c));
        }


        public static ZKPath Servers(String city) {
            return cache.root.computeIfAbsent(city, c -> Root(c).append("servers"));
        }
    }

    static class ZKConnectionWrapper {
        ZKConnection zk;
        ZKConnectionWrapper(ZKConnection zk) { this.zk = zk; }

        void registerCityZNodes(String city) throws InterruptedException {
            try {
                zk.createPersistentPath(Shard.Path.Root(city));
                zk.createPersistentPath(Shard.Path.Servers(city));
            } catch (KeeperException e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
        }

        String registerCityServer(String city) throws InterruptedException {
            var srvNode = Shard.Path.Servers(city).append("server");
            var mode = CreateMode.EPHEMERAL_SEQUENTIAL;
            ZKPath created = null;
            try {
                created = zk.createNode(srvNode, mode);
            } catch (KeeperException e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
            return created.get(created.length() - 1);
        }
    }
    public static ShardServer RegisterCityServer(ZKConnection zkCon, String city) throws InterruptedException {
        var zk = new ZKConnectionWrapper(zkCon);
        zk.registerCityZNodes(city);
        var id = zk.registerCityServer(city);
        return null ; //new ShardServer(city, id, zkCon);
    }
}
