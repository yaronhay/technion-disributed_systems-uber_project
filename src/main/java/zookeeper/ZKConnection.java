package zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import utils.Host;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class ZKConnection {
    static final Logger log = LogManager.getLogger();
    final List<ACL> ALL_PERMISSIONS = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    final int sessionTimout = 5000;
    final ZooKeeper zk;


    CountDownLatch connectedSync;
    public ZKConnection(List<Host> hosts) throws IOException {
        connectedSync = new CountDownLatch(1);
        this.zk = new ZooKeeper(
                Host.hostList(hosts),
                sessionTimout,
                this::connectionWatcher);

    }

    public boolean connectedSync() {
        try {
            this.connectedSync.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (zk.getState().isConnected()) {
            log.info("Connected to ZooKeeper server: {}", zk.toString());
            return true;
        } else {
            log.error("Not Connected to ZooKeeper server: {}", zk.toString());
            return false;
        }
    }

    void connectionWatcher(WatchedEvent watchedEvent) {
        var state = watchedEvent.getState();
        log.debug("Default Watcher Event: Path {} Type {} State {}",
                watchedEvent.getPath(),
                watchedEvent.getType().toString(),
                state.toString());

        if (state == Watcher.Event.KeeperState.SyncConnected) {
            this.connectedSync.countDown();
        }

    }

    public void createPersistentPath(ZKPath zkPath)
            throws InterruptedException, KeeperException {
        for (int i = 0; i < zkPath.length(); i++) {  // i = 1 to skip teh first prefix
            var subpath = zkPath.prefix(i);
            if (!this.nodeExists(subpath)) {
                log.debug("Path Create Node {} Does not Exists", subpath.str());
                this.createRegularNode(subpath);
            } else {
                log.debug("Path Create Node {} Exists", subpath.str());
            }
        }

    }

    public boolean nodeExists(ZKPath path) throws KeeperException, InterruptedException {
        return this.zk.exists(path.str(), false) != null;
    }

    public ZKPath createRegularNode(ZKPath node) throws KeeperException, InterruptedException {
//        var mode = CreateMode.PERSISTENT;
//        byte[] emptyData = {};
//        var created = this.zk.create(node.str(), emptyData, ALL_PERMISSIONS, mode);
//        return ZKPath.fromStr(created);
        log.debug("Creating ZNode : {}", node.str());
        try {
            var node1 = this.createNode(node, CreateMode.PERSISTENT);
            log.debug("Created ZNode : {}", node.str());
            return node1;
        } catch (KeeperException e) {
            log.debug("Error when Creating ZNode : {} {}", node.str(), e.toString());
            throw e;
        }
    }

    public ZKPath createNode(ZKPath node, CreateMode mode, byte[] data) throws KeeperException, InterruptedException {
        var created = this.zk.create(node.str(), data, ALL_PERMISSIONS, mode);
        return ZKPath.fromStr(created);
    }

    public ZKPath createNode(ZKPath node, CreateMode mode) throws KeeperException, InterruptedException {
        return createNode(node, mode, new byte[]{});
    }

    public void addPersistentWatch(ZKPath path, Watcher w) throws KeeperException, InterruptedException {
        this.zk.addWatch(path.str(), w, AddWatchMode.PERSISTENT);
    }

    public void addPersistentRecursiveWatch(ZKPath path, Watcher w) throws KeeperException, InterruptedException {
        this.zk.addWatch(path.str(), w, AddWatchMode.PERSISTENT_RECURSIVE);
    }

    public byte[] getData(ZKPath node) throws KeeperException, InterruptedException {
        return this.zk.getData(node.str(), false, null);
    }

    public List<ZKPath> getChildren(ZKPath node) throws KeeperException, InterruptedException {
        return this.zk
                .getChildren(node.str(), false)
                .stream()
                .map(node::append)
                .collect(Collectors.toList());
    }

    public void close() {
        try {
            this.zk.close();
        } catch (InterruptedException e) {
            log.error("Interrupted exception upon ZK connection closing", e);
        }
    }
}
