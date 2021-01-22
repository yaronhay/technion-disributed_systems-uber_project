package utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Random {
    static final java.util.Random generator = new java.util.Random();

    public static <K, V> K getRandomKey(Map<K, V> map) {
        var keys = map.keySet().toArray();
        if (keys.length == 0) {
            return null;
        }
        var idx = generator.nextInt(keys.length);

        return (K) keys[idx];
    }

    public static <K, V> Set<K> getRandomKeys(Map<K, V> map, int k) {
        Set<K> set = new HashSet<>();
        var keys = map.keySet().toArray();
        for (int i = 0; i < k; i++) {
            K key = (K) keys[generator.nextInt(keys.length)];
            set.add(key);
        }
        return set;
    }

}
