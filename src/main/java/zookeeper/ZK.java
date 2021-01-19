package zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;

public class ZK {
    public static ZKPath Path(String... path) {
        return new ZKPath(path);
    }


    public static class Op {
        public static org.apache.zookeeper.Op createNode(ZKPath node, CreateMode mode, byte[] data) {
            return org.apache.zookeeper.Op.create(node.str(), data, ZKConnection.ALL_PERMISSIONS, mode);
        }

        public static org.apache.zookeeper.Op getData(ZKPath node) { return org.apache.zookeeper.Op.getData(node.str()); }

        public static org.apache.zookeeper.Op createNode(ZKPath node, CreateMode mode) {
            return createNode(node, mode, new byte[]{});
        }

        public static org.apache.zookeeper.Op delete(ZKPath node, int version) {
            return org.apache.zookeeper.Op.delete(node.str(), version);
        }

        public static org.apache.zookeeper.Op delete(ZKPath node) {
            return delete(node, -1);
        }
    }
}
