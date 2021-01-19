package zookeeper;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;


@FunctionalInterface
public interface VoidCallback {
    class Adapter implements AsyncCallback.VoidCallback {
        final zookeeper.VoidCallback callback;
        public Adapter(zookeeper.VoidCallback callback) {this.callback = callback;}

        @Override public void processResult(int rc, String path, Object ctx) {
            var code = KeeperException.Code.get(rc);
            callback.processResult(code);
        }
    }
    void processResult(KeeperException.Code rc);
}