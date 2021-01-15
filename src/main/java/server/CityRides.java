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
    private Map<String, UUID> schedule;
    private Map<UUID, Ride> rides;

    private UUID city;

    public CityRides() {
        this.reservations = new ConcurrentHashMap<>();
        this.schedule = new ConcurrentHashMap<>();
        this.rides = new ConcurrentHashMap<>();
    }

    static String fromDate(Date date) {
        return String.format("%02d/%02d/%04d", date.getDay(), date.getMonth(), date.getYear());
    }

    public void addRide(UUID rideID, Ride ride) {
        var dateKey = fromDate(ride.getDate());
        this.rides.putIfAbsent(rideID, ride);
        this.schedule.putIfAbsent(dateKey, rideID);
    }

    public boolean hasRide(UUID rideID) {
        return this.rides.containsKey(rideID);
    }
}