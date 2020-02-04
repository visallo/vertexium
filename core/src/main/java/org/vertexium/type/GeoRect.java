package org.vertexium.type;

import static org.vertexium.util.Preconditions.checkNotNull;

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
