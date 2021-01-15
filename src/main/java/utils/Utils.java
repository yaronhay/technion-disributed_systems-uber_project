package utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Utils {
    static final Logger log = LogManager.getLogger();

    public static boolean sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
            return true;
        } catch (InterruptedException e) {
            log.warn("Sleep Interrupted exception", e);
            return false;
        }
    }
}
