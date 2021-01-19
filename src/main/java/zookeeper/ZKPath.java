package zookeeper;


import org.javatuples.Pair;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

class Pointer<T> {
    public T val;
}

public class ZKPath {

    private static class Root extends ZKPath {
        public Root() { super(null, null); }

        @Override protected Pair<StringBuilder, Integer> pathPrefixSB(int nodeCount) {
            return Pair.with(new StringBuilder(), nodeCount);
        }

        @Override public int length() { return 0; }

        @Override public String toString() {
            return "ROOT";
        }
        @Override public ZKPath prefix(int nodeCount) {
            throw new UnsupportedOperationException("Root prefix");
        }
    }

    private final ZKPath parent;
    private final List<String> zknodes;
    private final int _length;


    public ZKPath(String... path) {
        this(Arrays.asList(path));
    }
    public ZKPath(List<String> zknodes) { this(new Root(), zknodes); }
    private ZKPath(ZKPath parent, List<String> zknodes) {
        this.parent = parent;

        if (zknodes == null) {
            this.zknodes = new ArrayList<>();
        } else {
            this.zknodes = new ArrayList<>(zknodes);
        }

        int len = this.zknodes.size();
        if (this.parent != null) {
            len += this.parent.length();
        }
        this._length = len;
    }

    public int length() { return this._length; }

    public ZKPath append(List<String> zknodes) {
        return new ZKPath(this, zknodes);
    }
    public ZKPath append(String... zknodes) {
        return new ZKPath(this, Arrays.asList(zknodes));
    }

    protected Pair<StringBuilder, Integer> pathPrefixSB(int nodeCount) {
        var ret = this.parent.pathPrefixSB(nodeCount);
        var sb = ret.getValue0();

        nodeCount = ret.getValue1();

        for (int i = 0; i < this.zknodes.size() && nodeCount > 0; i++) {
            sb.append('/');
            sb.append(this.zknodes.get(i));
            nodeCount--;
        }
        return Pair.with(sb, nodeCount);
    }


    public ZKPath prefix(int i) {
        var nc = i + 1;
        var pref = new Pointer<BiFunction<Integer, ZKPath, ZKPath>>();
        pref.val = (nodeCount, thisPath) -> {
            if (nodeCount == 0) {
                return new ZKPath();
            } else if (nodeCount <= thisPath.parent.length()) {
                return pref.val.apply(nodeCount, thisPath.parent);
            } else if (nodeCount == this.length()) {
                return thisPath;
            } else if (nodeCount < this.length()) {
                var count = this.length() - this.parent.length();
                return new ZKPath(
                        this.parent,
                        this.zknodes.subList(0, count - 1)
                );
            } else {
                throw new IndexOutOfBoundsException("Prefix length is longer than size of path");
            }
        };
        return pref.val.apply(nc, this);
    }

    public String get(int idx) {
        if (idx >= this.length()) {
            throw new IndexOutOfBoundsException("Out of bounds");
        }

        var getNode = new Pointer<Function<ZKPath, String>>();
        getNode.val = (zkPath) -> {
            if (zkPath.parent.length() - 1 < idx) {
                String res = zkPath.zknodes.get(idx - zkPath.parent.length());
                return res;
            } else {
                return getNode.val.apply(zkPath.parent);
            }
        };
        return getNode.val.apply(this);
    }

    private String strCache = null;
    public String str() {
        if (strCache == null) {
            strCache = switch (this.length()) {
                case 0 -> "/";
                default -> this
                        .pathPrefixSB(this.length())
                        .getValue0()
                        .toString();
            };
        }
        return strCache;
    }
    @Override public String toString() {
        return String.join(
                "", this.parent.toString(), ";", this.zknodes.toString());
    }

    public static void main(String[] args) {
        /*var path = new ZKPath("cities", "A");
        System.out.println(path.str());

        var path2 = path.append("servers");
        System.out.println(path2.str());
        System.out.println(path2.prefix(3).str());

        var path3 = path2.append().append("Server00000");
        System.out.println(path3.prefix(3).str());

        for (int i = 0; i < path3.length(); i++) {
            System.out.println(path3.get(i));
        }

        */
//        var shard = new UUID(0, 1);
//        var zkPath = ZK.Path("shard_" + shard.toString(), "servers");
//        for (int i = 0; i < zkPath.length(); i++) {  // i = 1 to skip teh first prefix
//            var subpath = zkPath.prefix(i);
//            System.out.println(subpath.str());
//        }

        var zkPath = ZK.Path("shards").append("shard").append("servers");
        for (int i = 0; i < zkPath.length(); i++) {  // i = 1 to skip teh first prefix
            var subpath = zkPath.prefix(i);
            System.out.println(subpath.str());
        }
        System.out.println(zkPath.prefix(zkPath.length()-2).str());

    }

    public static ZKPath fromStr(String s) {
        return new ZKPath(
                Arrays.stream(s.split("/"))
                        .filter(str -> str != "")
                        .collect(Collectors.toList())
        );
    }
}
