package org.vertexium.type;

public class GeoRect implements GeoShape {
    private final GeoPoint northWest;
    private final GeoPoint southEast;

    public GeoRect(GeoPoint northWest, GeoPoint southEast) {
        this.northWest = northWest;
        this.southEast = southEast;
    }

    @Override
    public boolean within(GeoShape geoShape) {
        throw new RuntimeException("not supported");
    }

    public GeoPoint getNorthWest() {
        return northWest;
    }

    public GeoPoint getSouthEast() {
        return southEast;
    }

    @Override
    public String toString() {
        return "[" + getNorthWest() + "," + getSouthEast() + "]";
    }
}
