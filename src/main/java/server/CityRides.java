package server;


import uber.proto.objects.Date;
import uber.proto.objects.Ride;
import uber.proto.objects.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class CityRides {
    private Map<UUID, List<User>> reservations;
    private Map<Date, Ride> schedule;

    private final Object lock;

    private UUID city;

    public CityRides() {
        this.reservations = new ConcurrentHashMap<>();
        this.schedule = new ConcurrentHashMap<>();
        this.lock = new Object();
    }


    public void addRide(UUID rideID, Ride ride) {
        synchronized (this.lock) {
            this.schedule.putIfAbsent(ride.getDate(), ride); // Todo tostring
            reservations.putIfAbsent(rideID, new ArrayList<>(ride.getVacancies()));
        }
    }

}