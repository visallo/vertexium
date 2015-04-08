package org.vertexium.type;

import org.vertexium.VertexiumException;

import java.io.Serializable;

public class GeoCircle implements Serializable, GeoShape {
    private static final long serialVersionUID = 1L;
    private final double latitude;
    private final double longitude;
    private final double radius;

    public GeoCircle(double latitude, double longitude, double radius) {
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
    public boolean within(GeoShape geoShape) {
        if (geoShape instanceof GeoPoint) {
            GeoPoint pt = (GeoPoint) geoShape;
            return GeoPoint.distanceBetween(getLatitude(), getLongitude(), pt.getLatitude(), pt.getLongitude()) <= getRadius();
        }
        throw new VertexiumException("Not implemented for argument type " + geoShape.getClass().getName());
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
        final GeoCircle other = (GeoCircle) obj;
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
