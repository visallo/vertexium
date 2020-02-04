package org.vertexium.type;

public class GeoCircle extends GeoShapeBase {
    private static final long serialVersionUID = 1L;
    private final double latitude;
    private final double longitude;
    private final double radius;

    /**
     * @param latitude  latitude is specified in decimal degrees
     * @param longitude longitude is specified in decimal degrees
     * @param radius    radius is specified in kilometers
     */
    public GeoCircle(double latitude, double longitude, double radius) {
        this(latitude, longitude, radius, null);
    }

    /**
     * @param latitude    latitude is specified in decimal degrees
     * @param longitude   longitude is specified in decimal degrees
     * @param radius      radius is specified in kilometers
     * @param description name or description of this shape
     */
    public GeoCircle(double latitude, double longitude, double radius, String description) {
        super(description);
        this.latitude = latitude;
        this.longitude = longitude;
        this.radius = radius;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getRadius() {
        return radius;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 61 * hash + (int) (Double.doubleToLongBits(this.latitude) ^ (Double.doubleToLongBits(this.latitude) >>> 32));
        hash = 61 * hash + (int) (Double.doubleToLongBits(this.longitude) ^ (Double.doubleToLongBits(this.longitude) >>> 32));
        hash = 61 * hash + (int) (Double.doubleToLongBits(this.radius) ^ (Double.doubleToLongBits(this.radius) >>> 32));
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
        GeoCircle other = (GeoCircle) obj;
        if (Double.doubleToLongBits(this.latitude) != Double.doubleToLongBits(other.latitude)) {
            return false;
        }
        if (Double.doubleToLongBits(this.longitude) != Double.doubleToLongBits(other.longitude)) {
            return false;
        }
        if (Double.doubleToLongBits(this.radius) != Double.doubleToLongBits(other.radius)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "GeoCircle[" + getLatitude() + ", " + getLongitude() + ", " + getRadius() + "]";
    }
}
