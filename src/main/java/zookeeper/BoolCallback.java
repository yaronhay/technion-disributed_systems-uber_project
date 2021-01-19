package zookeeper;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

@FunctionalInterface
public interface BoolCallback {
    class Adapter implements AsyncCallback.StatCallback {
        final BoolCallback callback;
        public Adapter(BoolCallback callback) {this.callback = callback;}


        @Override public void processResult(int rc, String path, Object ctx, Stat stat) {
            var code = KeeperException.Code.get(rc);
            callback.processResult(code, code == KeeperException.Code.OK);
        }
    }
    void processResult(KeeperException.Code rc, boolean exists);
}