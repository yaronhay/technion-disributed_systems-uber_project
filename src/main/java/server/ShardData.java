package server;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uber.proto.objects.Ride;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ShardData {
    static final Logger log = LogManager.getLogger();

    private final Map<UUID, CityRides> cities;

    // Will be used in the opposite way (ONE READER and MULTIPLE WRITERS)
    private final ReadWriteLock lock;

    public ShardData() {
        this.cities = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock(true);
    }

    CityRides get(UUID city) {
        return this.cities.computeIfAbsent(city, c -> new CityRides());
    }

    public void addRide(UUID rideID, Ride ride) {
        this.lock.readLock().lock();
        try {
            UUID cityID = utils.UUID.fromID(ride.getSource().getId());
            CityRides cityRides = this.get(cityID);

            if (!cityRides.hasRide(rideID)) {
                cityRides.addRide(rideID, ride);
            }
            log.info("Added ride {} to local database\n\t\t\t{}", rideID, ride);
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
