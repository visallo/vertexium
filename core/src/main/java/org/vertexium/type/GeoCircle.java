package org.vertexium.type;

import org.vertexium.VertexiumException;

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
    public boolean intersects(GeoShape geoShape) {
        if (geoShape instanceof GeoPoint) {
            return within(geoShape);
        } else if (geoShape instanceof GeoCircle) {
            GeoCircle circle = (GeoCircle) geoShape;
            double centerDistance = distanceBetween(latitude, longitude, circle.latitude, circle.longitude);
            return centerDistance < (radius + circle.radius);
        }
        throw new VertexiumException("Not implemented for argument type " + geoShape.getClass().getName());
    }

    @Override
    public boolean within(GeoShape geoShape) {
        if (geoShape instanceof GeoPoint) {
            GeoPoint pt = (GeoPoint) geoShape;
            return distanceBetween(getLatitude(), getLongitude(), pt.getLatitude(), pt.getLongitude()) <= getRadius();
        } else if (geoShape instanceof GeoCircle) {
            GeoCircle circle = (GeoCircle) geoShape;
            double distance = distanceBetween(getLatitude(), getLongitude(), circle.getLatitude(), circle.getLongitude());
            return distance <= getRadius() + circle.getRadius();
        } else if (geoShape instanceof GeoRect) {
            GeoRect rect = (GeoRect) geoShape;
            return within(rect.getNorthWest()) && within(rect.getSouthEast());
        } else if (geoShape instanceof GeoHash) {
            return within(((GeoHash) geoShape).toGeoRect());
        } else if (geoShape instanceof GeoLine) {
            return ((GeoLine) geoShape).getGeoPoints().stream().allMatch(this::within);
        }
        throw new VertexiumException("Not implemented for argument type " + geoShape.getClass().getName());
    }

    // see http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates
    public GeoRect getBoundingBox() {
        double radDist = radius / EARTH_RADIUS;
        double radLat = Math.toRadians(latitude);
        double radLon = Math.toRadians(longitude);

        double minLat = radLat - radDist;
        double maxLat = radLat + radDist;

        double minLon, maxLon;
        if (minLat > MIN_LAT && maxLat < MAX_LAT) {
            double deltaLon = Math.asin(Math.sin(radDist) /
                                                Math.cos(radLat));
            minLon = radLon - deltaLon;
            if (minLon < MIN_LON) minLon += 2d * Math.PI;
            maxLon = radLon + deltaLon;
            if (maxLon > MAX_LON) maxLon -= 2d * Math.PI;
        } else {
            // a pole is within the distance
            minLat = Math.max(minLat, MIN_LAT);
            maxLat = Math.min(maxLat, MAX_LAT);
            minLon = MIN_LON;
            maxLon = MAX_LON;
        }

        return new GeoRect(
                new GeoPoint(Math.toDegrees(maxLat), Math.toDegrees(minLon)),
                new GeoPoint(Math.toDegrees(minLat), Math.toDegrees(maxLon))
        );
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
