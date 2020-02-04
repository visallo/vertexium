package org.vertexium.type;

import org.vertexium.VertexiumException;

import java.util.ArrayList;
import java.util.List;

public class GeoCollection extends GeoShapeBase {
    private static final long serialVersionUID = -7001933739481434807L;
    private List<GeoShape> geoShapes = new ArrayList<>();

    public GeoCollection() {
    }

    public GeoCollection(String description) {
        super(description);
    }

    public GeoCollection(List<GeoShape> geoShapes) {
        this.geoShapes.addAll(geoShapes);
    }

    public GeoCollection(List<GeoShape> geoShapes, String description) {
        super(description);
        this.geoShapes.addAll(geoShapes);
    }

    @Override
    public boolean intersects(GeoShape geoShape) {
        return geoShapes.stream().anyMatch(shape -> shape.intersects(geoShape));
    }

    @Override
    public boolean within(GeoShape geoShape) {
        return geoShapes.stream().allMatch(shape -> shape.within(geoShape));
    }

    public GeoCollection addShape(GeoShape geoShape) {
        geoShapes.add(geoShape);
        return this;
    }

    public GeoCollection addShapes(List<GeoShape> geoShapes) {
        this.geoShapes.addAll(geoShapes);
        return this;
    }

    public List<GeoShape> getGeoShapes() {
        return geoShapes;
    }

    @Override
    public void validate() {
        if (geoShapes.size() < 1) {
            throw new VertexiumException("A GeoCollection must contain at least one shape.");
        }
        geoShapes.forEach(GeoShape::validate);
    }

    @Override
    public int hashCode() {
        return geoShapes.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return geoShapes.equals(((GeoCollection) obj).geoShapes);
    }
}
