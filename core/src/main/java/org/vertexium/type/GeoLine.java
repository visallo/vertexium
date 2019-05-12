package org.vertexium.type;

import org.vertexium.VertexiumException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class GeoLine extends GeoShapeBase {
    private static final long serialVersionUID = 5523982042809683074L;
    private List<GeoPoint> geoPoints = new ArrayList<>();

    public GeoLine(List<GeoPoint> geoPoints) {
        this.geoPoints.addAll(geoPoints);
        this.validate();
    }

    public GeoLine(List<GeoPoint> geoPoints, String description) {
        super(description);
        this.geoPoints.addAll(geoPoints);
        this.validate();
    }

    public GeoLine(GeoPoint start, GeoPoint end) {
        checkNotNull(start, "start is required");
        checkNotNull(end, "end is required");
        geoPoints.add(start);
        geoPoints.add(end);
    }

    public GeoLine(GeoPoint start, GeoPoint end, String description) {
        super(description);
        checkNotNull(start, "start is required");
        checkNotNull(end, "end is required");
        geoPoints.add(start);
        geoPoints.add(end);
    }

    @Override
    public boolean intersects(GeoShape geoShape) {
        throw new VertexiumException("Not implemented for argument type " + geoShape.getClass().getName());
    }

    @Override
    public boolean within(GeoShape geoShape) {
        throw new VertexiumException("Not implemented for argument type " + geoShape.getClass().getName());
    }

    @Override
    public void validate() {
        if (geoPoints.size() < 2) {
            throw new VertexiumException("A GeoLine must have at least two points.");
        }
    }

    public GeoLine addGeoPoint(GeoPoint geoPoint) {
        geoPoints.add(geoPoint);
        return this;
    }

    public GeoLine addGeoPoints(List<GeoPoint> geoPoints) {
        this.geoPoints.addAll(geoPoints);
        return this;
    }

    public void setGeoPoints(List<GeoPoint> geoPoints) {
        geoPoints.clear();
        this.geoPoints.addAll(geoPoints);
    }

    public List<GeoPoint> getGeoPoints() {
        return geoPoints;
    }

    @Override
    public int hashCode() {
        return geoPoints.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return geoPoints.equals(((GeoLine) obj).geoPoints);
    }

    @Override
    public String toString() {
        return "GeoLine[" + geoPoints.stream().map(Object::toString).collect(Collectors.joining(", ")) + "]";
    }
}
