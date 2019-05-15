package org.vertexium.type;

import org.vertexium.VertexiumException;

import static com.google.common.base.Preconditions.checkNotNull;

public class GeoRect extends GeoShapeBase {
    private static final long serialVersionUID = 7701255671989639309L;
    private final GeoPoint northWest;
    private final GeoPoint southEast;

    public GeoRect(GeoPoint northWest, GeoPoint southEast) {
        checkNotNull(northWest, "northWest is required");
        checkNotNull(southEast, "southEast is required");
        this.northWest = northWest;
        this.southEast = southEast;
    }

    public GeoRect(GeoPoint northWest, GeoPoint southEast, String description) {
        super(description);
        checkNotNull(northWest, "northWest is required");
        checkNotNull(southEast, "southEast is required");
        this.northWest = northWest;
        this.southEast = southEast;
    }

    @Override
    public boolean intersects(GeoShape geoShape) {
        if (geoShape instanceof GeoPoint) {
            return within(geoShape);
        } else if (geoShape instanceof GeoRect) {
            GeoRect rect = (GeoRect) geoShape;
            return getNorthWest().isNorthWestOf(rect.getSouthEast())
                && getSouthEast().isSouthEastOf(rect.getNorthWest());
        } else if (geoShape instanceof GeoHash) {
            return intersects(((GeoHash) geoShape).toGeoRect());
        }
        throw new VertexiumException("Not implemented for argument type " + geoShape.getClass().getName());
    }

    @Override
    public boolean within(GeoShape geoShape) {
        if (geoShape instanceof GeoPoint) {
            GeoPoint pt = (GeoPoint) geoShape;
            return pt.isSouthEastOf(getNorthWest())
                && pt.isNorthWestOf(getSouthEast());
        } else if (geoShape instanceof GeoRect) {
            GeoRect rect = (GeoRect) geoShape;
            return getNorthWest().isNorthWestOf(rect.getNorthWest())
                && getSouthEast().isSouthEastOf(rect.getSouthEast());
        } else if (geoShape instanceof GeoHash) {
            return within(((GeoHash) geoShape).toGeoRect());
        } else if (geoShape instanceof GeoCircle) {
            return within(((GeoCircle) geoShape).getBoundingBox());
        } else if (geoShape instanceof GeoLine) {
            return ((GeoLine) geoShape).getGeoPoints().stream().allMatch(this::within);
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
    public int hashCode() {
        int hash = 11;
        hash = 61 * hash + northWest.hashCode();
        hash = 61 * hash + southEast.hashCode();
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
        final GeoRect other = (GeoRect) obj;
        return northWest.equals(other.northWest) && southEast.equals(other.southEast);
    }

    @Override
    public String toString() {
        return "GeoRect[" + getNorthWest() + "," + getSouthEast() + "]";
    }
}
