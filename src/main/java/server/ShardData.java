package server;


import uber.proto.objects.Ride;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ShardData {
    private final Map<UUID, CityRides> cities;

    // Will be used in the opposite way (ONE READER and MULTIPLE WRITERS)
    private final ReadWriteLock readWriteLock;

    public ShardData() {
        this.cities = new ConcurrentHashMap<>();
        this.readWriteLock = new ReentrantReadWriteLock(true);
    }

    CityRides get(UUID city) {
        return this.cities.computeIfAbsent(city, c -> new CityRides());
    }

    public void addRide(UUID rideID, Ride ride) {

    }
}
