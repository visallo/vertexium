package org.vertexium.type;

import org.vertexium.VertexiumException;

public class GeoRect implements GeoShape {
    private final GeoPoint northWest;
    private final GeoPoint southEast;

    public GeoRect(GeoPoint northWest, GeoPoint southEast) {
        this.northWest = northWest;
        this.southEast = southEast;
    }

    @Override
    public boolean within(GeoShape geoShape) {
        if (geoShape instanceof GeoPoint) {
            GeoPoint pt = (GeoPoint) geoShape;
            return pt.isSouthEastOf(getNorthWest())
                    && pt.isNorthWestOf(getSouthEast());
        }
        throw new VertexiumException("Not implemented for argument type " + geoShape.getClass().getName());
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

    public boolean intersect(GeoShape geoShape) {
        if (geoShape instanceof GeoRect) {
            GeoRect rect = (GeoRect) geoShape;
            return getNorthWest().isNorthWestOf(rect.getSouthEast())
                    && getSouthEast().isSouthEastOf(rect.getNorthWest());
        }
        throw new VertexiumException("Not implemented for argument type " + geoShape.getClass().getName());
    }
}
