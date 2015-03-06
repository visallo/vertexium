package org.neolumin.vertexium.type;

import org.neolumin.vertexium.VertexiumException;

import java.io.Serializable;

public class GeoPoint implements Serializable, GeoShape, Comparable<GeoPoint> {
    private static final long serialVersionUID = 1L;
    private static final double COMPARE_TOLERANCE = 0.00001;
    private static double EARTH_RADIUS = 6371; // km
    private double latitude;
    private double longitude;
    private Double altitude;
    private String description;

    protected GeoPoint() {
        latitude = 0;
        longitude = 0;
        altitude = null;
        description = null;
    }

    public GeoPoint(double latitude, double longitude, Double altitude, String description) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
        this.description = description;
    }

    public GeoPoint(double latitude, double longitude, Double altitude) {
        this(latitude, longitude, altitude, null);
    }

    public GeoPoint(double latitude, double longitude) {
        this(latitude, longitude, null, null);
    }

    public GeoPoint(double latitude, double longitude, String description) {
        this(latitude, longitude, null, description);
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public Double getAltitude() {
        return altitude;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "(" + getLatitude() + ", " + getLongitude() + ")";
    }

    @Override
    public boolean within(GeoShape geoShape) {
        throw new VertexiumException("Not implemented for argument type " + geoShape.getClass().getName());
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + (int) (Double.doubleToLongBits(this.latitude) ^ (Double.doubleToLongBits(this.latitude) >>> 32));
        hash = 47 * hash + (int) (Double.doubleToLongBits(this.longitude) ^ (Double.doubleToLongBits(this.longitude) >>> 32));
        hash = 47 * hash + (this.altitude != null ? this.altitude.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final GeoPoint other = (GeoPoint) obj;
        if (Double.doubleToLongBits(this.latitude) != Double.doubleToLongBits(other.latitude)) {
            return false;
        }
        if (Double.doubleToLongBits(this.longitude) != Double.doubleToLongBits(other.longitude)) {
            return false;
        }
        if (this.altitude != other.altitude && (this.altitude == null || !this.altitude.equals(other.altitude))) {
            return false;
        }
        return true;
    }

    public static double distanceBetween(GeoPoint geoPoint1, GeoPoint geoPoint2) {
        return distanceBetween(
                geoPoint1.getLatitude(), geoPoint1.getLongitude(),
                geoPoint2.getLatitude(), geoPoint2.getLongitude());
    }

    // see http://www.movable-type.co.uk/scripts/latlong.html
    public static double distanceBetween(double latitude1, double longitude1, double latitude2, double longitude2) {
        double dLat = toRadians(latitude2 - latitude1);
        double dLon = toRadians(longitude2 - longitude1);
        latitude1 = toRadians(latitude1);
        latitude2 = toRadians(latitude2);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(latitude1) * Math.cos(latitude2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS * c;
    }

    private static double toRadians(double v) {
        return v * Math.PI / 180;
    }

    @Override
    public int compareTo(GeoPoint other) {
        int i;
        if ((i = compare(getLatitude(), other.getLatitude())) != 0) {
            return i;
        }
        if ((i = compare(getLongitude(), other.getLongitude())) != 0) {
            return i;
        }
        if (getAltitude() != null && other.getAltitude() != null) {
            return compare(getAltitude(), other.getAltitude());
        }
        if (getAltitude() != null) {
            return 1;
        }
        if (other.getAltitude() != null) {
            return -1;
        }
        return 0;
    }

    private static int compare(double d1, double d2) {
        if (Math.abs(d1 - d2) < COMPARE_TOLERANCE) {
            return 0;
        }
        if (d1 < d2) {
            return -1;
        }
        if (d1 > d2) {
            return 1;
        }
        return 0;
    }
}
