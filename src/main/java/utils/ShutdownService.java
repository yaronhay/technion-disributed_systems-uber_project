package utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ShutdownService {
    static final Logger log = LogManager.getLogger();

    private static AtomicBoolean allowed;
    private static final Queue<Runnable> hooks;

    static {
        hooks = new ConcurrentLinkedQueue<>();
        Runtime.getRuntime().addShutdownHook(new Thread(ShutdownService::shutdown));
    }

    public static void addHook(Runnable r) {
        hooks.add(r);
    }
    static void shutdown() {
        log.info("Running all shutdown hooks");

        for (var hook : hooks) {
            hook.run();
        }

        log.info("Closing logging service");
        LogManager.shutdown();
    }
}
