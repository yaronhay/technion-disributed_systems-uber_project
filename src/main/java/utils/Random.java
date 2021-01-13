package utils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Random {
    static final java.util.Random generator = new java.util.Random();

    public static <K, V> K getRandomKey(Map<K, V> map) {
        var keys = map.keySet().toArray();
        return (K) keys[generator.nextInt(keys.length)];
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
