package utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uber.proto.objects.Date;
import uber.proto.objects.Reservation;

import java.util.List;

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

    public static double distancePointSegment(long x_1, long y_1,
                                              long x_2, long y_2,
                                              long x_p, long y_p) {
        var a1 = (x_2 - x_1) * (y_1 - y_p);
        var a2 = (x_1 - x_p) * (y_2 - y_1);

        var rt = (x_2 - x_1) * (x_2 - x_1) + (y_2 - y_1) * (y_2 - y_1);

        var abs = Math.abs(a1 - a2);
        var sqrt = Math.sqrt(rt);
        var res = abs / sqrt;
        // System.out.printf("(%d,%d):(%d,%d) point (%d,%d) abs(%d)/sqrt(%fl)=(%fl)\n",
        //        x_1, y_1, x_2, y_2, x_p, y_p, abs, sqrt, res);
        return res;

    }
    public static String dateAsStr(Date date) {
        return String.format("%02d/%02d/%04d", date.getDay(), date.getMonth(), date.getYear());
    }


    public static <T> void ensureListSize(List<T> l, int size) {
        while (l.size() < size) {
            l.add(null);
        }
    }
}
