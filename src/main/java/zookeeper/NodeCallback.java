package zookeeper;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;

@FunctionalInterface
public interface NodeCallback {
    class Adapter implements AsyncCallback.StringCallback {
        final NodeCallback callback;
        public Adapter(NodeCallback callback) {this.callback = callback;}

        @Override public void processResult(int rc, String path, Object ctx, String name) {
            ZKPath zkPath = ZKPath.fromStr(path);
            zkPath = zkPath.prefix(zkPath.length() - 2);
            zkPath = zkPath.append(name);

            var code = KeeperException.Code.get(rc);
            callback.processResult(code, zkPath);
        }
    }
    void processResult(KeeperException.Code rc, ZKPath path);
}
