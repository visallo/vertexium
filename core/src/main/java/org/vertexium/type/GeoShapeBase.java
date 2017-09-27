package org.vertexium.type;

import java.io.Serializable;

public abstract class GeoShapeBase implements GeoShape, Serializable {
    private static final long serialVersionUID = 6993185229233913152L;
    public static double EARTH_RADIUS = 6371; // km
    public static final double MIN_LAT = Math.toRadians(-90d);  // -PI/2
    public static final double MAX_LAT = Math.toRadians(90d);   //  PI/2
    public static final double MIN_LON = Math.toRadians(-180d); // -PI
    public static final double MAX_LON = Math.toRadians(180d);  //  PI

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

    // see http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates
    public double distanceBetween(double latitude1, double longitude1, double latitude2, double longitude2) {
        latitude1 = toRadians(latitude1);
        longitude1 = toRadians(longitude1);
        latitude2 = toRadians(latitude2);
        longitude2 = toRadians(longitude2);

        return Math.acos(Math.sin(latitude1) * Math.sin(latitude2) +
                Math.cos(latitude1) * Math.cos(latitude2) *
                        Math.cos(longitude1 - longitude2)) * EARTH_RADIUS;
    }

    public double toRadians(double v) {
        return v * Math.PI / 180;
    }

    public double fromRadians(double r) {
        return (r * 180) / Math.PI;
    }
}
