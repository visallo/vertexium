package org.vertexium.type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class GeoShapeBase implements GeoShape, Serializable {
    private static final long serialVersionUID = 6993185229233913152L;
    public static double EARTH_RADIUS = 6371; // km
    public static double EARTH_CIRCUMFERENCE = 2 * Math.PI * EARTH_RADIUS;
    public static final double MIN_LAT = Math.toRadians(-90d); // -PI/2
    public static final double MAX_LAT = Math.toRadians(90d); // PI/2
    public static final double MIN_LON = Math.toRadians(-180d); // -PI
    public static final double MAX_LON = Math.toRadians(180d); // PI

    private final String description;

    public GeoShapeBase() {
        description = null;
    }

    public GeoShapeBase(String description) {
        this.description = description;
    }

    @Override
    public void validate() {
    }

    @Override
    public String getDescription() {
        return description;
    }

    public double distanceBetween(double latitude1, double longitude1, double latitude2, double longitude2) {
        latitude1 = Math.toRadians(latitude1);
        longitude1 = Math.toRadians(longitude1);
        latitude2 = Math.toRadians(latitude2);
        longitude2 = Math.toRadians(longitude2);

        double cosLat1 = Math.cos(latitude1);
        double cosLat2 = Math.cos(latitude2);
        double sinLat1 = Math.sin(latitude1);
        double sinLat2 = Math.sin(latitude2);
        double deltaLon = longitude2 - longitude1;
        double cosDeltaLon = Math.cos(deltaLon);
        double sinDeltaLon = Math.sin(deltaLon);

        double a = cosLat2 * sinDeltaLon;
        double b = cosLat1 * sinLat2 - sinLat1 * cosLat2 * cosDeltaLon;
        double c = sinLat1 * sinLat2 + cosLat1 * cosLat2 * cosDeltaLon;

        double rads = Math.atan2(Math.sqrt(a * a + b * b), c);
        double percent = rads / (2 * Math.PI);
        return percent * EARTH_CIRCUMFERENCE;
    }

    /**
     * This is used to fix Kryo serialization issues with lists generated from methods such as
     * {@link java.util.Arrays#asList(Object[])}
     */
    protected <T> List<? extends List<T>> toArrayLists(List<List<T>> lists) {
        for (int i = 0; i < lists.size(); i++) {
            List<T> list = lists.get(i);
            lists.set(i, toArrayList(list));
        }
        return lists;
    }

    /**
     * This is used to fix Kryo serialization issues with lists generated from methods such as
     * {@link java.util.Arrays#asList(Object[])}
     */
    protected <T> List<T> toArrayList(List<T> list) {
        if (list == null) {
            return null;
        }
        if (!list.getClass().equals(ArrayList.class)) {
            list = new ArrayList<>(list);
        }
        return list;
    }
}
