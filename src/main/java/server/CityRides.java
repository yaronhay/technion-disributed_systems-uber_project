package server;


import uber.proto.objects.Date;
import uber.proto.objects.Ride;
import uber.proto.objects.User;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class CityRides {
    private Map<UUID, List<User>> rides;
    private Map<Date, Ride> schedule;

    private Object lock;

    private UUID city;

    public CityRides() {
        this.rides = new ConcurrentHashMap<>();
        this.schedule = new ConcurrentHashMap<>();
    }

}