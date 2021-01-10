package utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;

public class Host {
    public final InetAddress ip;
    public final int port;

    public Host(InetAddress ip, int port) {
        this.ip = ip;

        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid Port Number");
        }
        this.port = port;
    }
    public Host(String host, int port) throws UnknownHostException {
        this(InetAddress.getByName(host), port);
    }

    public static String hostList(List<Host> hostList) {
        var temp = hostList
                .stream()
                .map(host -> host.ip.getHostAddress() + ":" + host.port)
                .collect(Collectors.toList());
        return String.join(",", temp);
    }
}
