package zookeeper;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;

import java.util.List;

@FunctionalInterface
public interface ListCallback {
    class Adapter implements AsyncCallback.ChildrenCallback {
        final ListCallback callback;
        public Adapter(ListCallback callback) {this.callback = callback;}

        @Override public void processResult(int rc, String path, Object ctx, List<String> children) {
            var code = KeeperException.Code.get(rc);
            callback.processResult(code, children);
        }
    }
    void processResult(KeeperException.Code rc, List<String> children);
}
