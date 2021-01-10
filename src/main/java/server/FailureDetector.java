package server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import zookeeper.ZK;
import zookeeper.ZKConnection;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FailureDetector {
    static final Logger log = LogManager.getLogger();

    private final ZKConnection zk;
    private Map<Integer, Boolean> watching;

    public FailureDetector(ZKConnection zk) {
        this.zk = zk;
        this.watching = new ConcurrentHashMap<>();
    }

    public void register(int serverID) throws KeeperException, InterruptedException {
        var root = ZK.Path("servers");
        try {
            this.zk.createPersistentPath(root);
        } catch (KeeperException e) {
            log.error("Error upon creating servers root server id = " + serverID, e);
            throw e;
        }

        var fdNode = root.append("server_" + serverID);
        try {
            var created = this.zk.createNode(fdNode, CreateMode.EPHEMERAL);
            assert fdNode.str().equals(created.str());
        } catch(KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                throw new IllegalStateException("Unique node already exists");
            } else {
                throw e;
            }
        }
    }

    /*public void watch (int serverID) {

    }

    private void watch (WatchedEvent e) {

    }*/

    /*public void doIfNotSuspected(Runnable r, Set<Integer> servers) {
        Thread t = Thread.currentThread();

        r.run();
    }*/

}
