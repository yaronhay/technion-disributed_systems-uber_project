package server;


import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ShardData {
    private Map<UUID, CityRides> cities;
    private Object lock;

    public ShardData(Map<UUID, Boolean> cities) {
        this.cities = new ConcurrentHashMap<>();
        for (var city : cities.keySet()) {
            this.cities.put(city, new CityRides());
        }
        this.lock = new Object();
    }
}
