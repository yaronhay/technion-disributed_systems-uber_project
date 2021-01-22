package utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ShutdownService {
    static final Logger log = LogManager.getLogger();

    private static AtomicBoolean allowed;
    private static final Queue<Pair<Runnable, String>> hooks;

    static {
        hooks = new ConcurrentLinkedQueue<>();
        Runtime.getRuntime().addShutdownHook(new Thread(ShutdownService::shutdown));
    }

    public static void addHook(Runnable r, String msg) {
        hooks.add(Pair.with(r, msg));
    }
    static void shutdown() {
        log.info("Running all shutdown hooks");

        for (var hook : hooks) {
            var runnable = hook.getValue0();
            var msg = hook.getValue1();
            log.info("Starting shutdown hook for {}", msg);
            runnable.run();
            log.info("Ended shutdown hook for {}", msg);
        }

        log.info("Closing logging service");
        LogManager.shutdown();
    }
}
