package utils;

import org.apache.commons.lang.math.IntRange;
import org.json.JSONArray;
import org.json.JSONObject;
import uber.proto.objects.*;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JSONConverters {
    public static JSONObject toJSON(List<Ride> ridesList) {
        var arr = (new JSONArray())
                .putAll(ridesList
                        .stream()
                        .map(JSONConverters::toJSON)
                        .collect(Collectors.toList())
                );
        return (new JSONObject())
                .put("date", toJSON(ridesList.get(0).getDate()))
                .put("rides", arr);
    }
    public static JSONObject toJSON(Ride ride) {
        return (new JSONObject())
                .put("provider", toJSON(ride.getProvider()))
                .put("date", toJSON(ride.getDate()))
                .put("id", UUID.fromID(ride.getId()).toString())
                .put("source", toJSON(ride.getSource()))
                .put("destination", toJSON(ride.getDestination()));
    }
    public static JSONObject toJSON(User u) {
        return (new JSONObject())
                .put("firstname", u.getFirstName())
                .put("lastname", u.getLastName())
                .put("phonenumber", u.getPhoneNumber());
    }
    public static JSONObject toJSON(Date d) {
        return (new JSONObject())
                .put("day", d.getDay())
                .put("month", d.getMonth())
                .put("year", d.getYear());
    }
    public static JSONObject toJSON(City c) {
        return (new JSONObject())
                .put("id", UUID.fromID(c.getId()))
                .put("name", c.getName());
    }
    public static JSONObject toJSON(RideStatus rideStatus) {
        var ride = toJSON(rideStatus.getRide());
        var reservationsMap = rideStatus.getReservationsMap();

        var reservations = (new JSONArray())
                .putAll(IntStream
                        .range(1, rideStatus.getRide().getVacancies() + 1)
                        .mapToObj(i -> reservationsMap.get(i))
                        .map(u -> u != null ? toJSON(u) : null)
                        .collect(Collectors.toList()));
        return (new JSONObject())
                .put("ride-info", ride)
                .put("reservations", reservations);
    }
}
