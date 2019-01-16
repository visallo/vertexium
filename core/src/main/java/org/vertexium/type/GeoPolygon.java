package org.vertexium.type;

import org.vertexium.VertexiumException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GeoPolygon extends GeoShapeBase {
    private static final long serialVersionUID = 1341416821951387324L;
    private List<GeoPoint> outerBoundary = new ArrayList<>();
    private List<List<GeoPoint>> holeBoundaries = new ArrayList<>();

    /**
     * @param outerBoundary A list of geopoints that make up the outer boundary of this shape. To avoid ambiguity,
     *                      the points must be specified in counter-clockwise order. In addition, the first and last
     *                      point specified must match.
     */
    public GeoPolygon(List<GeoPoint> outerBoundary) {
        this.outerBoundary.addAll(outerBoundary);
        this.validate();
    }

    /**
     * @param outerBoundary A list of geopoints that make up the outer boundary of this shape. To avoid ambiguity,
     *                      the points must be specified in counter-clockwise order. In addition, the first and last
     *                      point specified must match.
     * @param description   name or description of this shape
     */
    public GeoPolygon(List<GeoPoint> outerBoundary, String description) {
        super(description);
        this.outerBoundary.addAll(outerBoundary);
        this.validate();
    }

    /**
     * @param outerBoundary  A list of geopoints that make up the outer boundary of this shape. To avoid ambiguity,
     *                       the points must be specified in counter-clockwise order. In addition, the first and last
     *                       point specified must match.
     * @param holeBoundaries A list of geopoint lists that make up the holes in this shape. To avoid ambiguity,
     *                       the points must be specified in clockwise order. In addition, the first and last
     *                       point specified must match.
     */
    public GeoPolygon(List<GeoPoint> outerBoundary, List<List<GeoPoint>> holeBoundaries) {
        this.outerBoundary.addAll(outerBoundary);
        this.holeBoundaries.addAll(toArrayLists(holeBoundaries));
        this.validate();
    }

    /**
     * @param outerBoundary  A list of geopoints that make up the outer boundary of this shape. To avoid ambiguity,
     *                       the points must be specified in counter-clockwise order. In addition, the first and last
     *                       point specified must match.
     * @param holeBoundaries A list of geopoint lists that make up the holes in this shape. To avoid ambiguity,
     *                       the points must be specified in clockwise order. In addition, the first and last
     *                       point specified must match.
     * @param description    name or description of this shape
     */
    public GeoPolygon(List<GeoPoint> outerBoundary, List<List<GeoPoint>> holeBoundaries, String description) {
        super(description);
        this.outerBoundary.addAll(outerBoundary);
        this.holeBoundaries.addAll(toArrayLists(holeBoundaries));
        this.validate();
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
        if (this.outerBoundary.size() < 4) {
            throw new VertexiumException("A polygon must specify at least 4 points for its outer boundary.");
        }
        if (!this.outerBoundary.get(0).equals(this.outerBoundary.get(this.outerBoundary.size() - 1))) {
            throw new VertexiumException("A polygon outer boundary must begin and end at the same point.");
        }
        this.holeBoundaries.forEach(holeBoundary -> {
            if (holeBoundary.size() < 4) {
                throw new VertexiumException("A polygon hole must specify at least 4 points.");
            }
            if (!holeBoundary.get(0).equals(holeBoundary.get(holeBoundary.size() - 1))) {
                throw new VertexiumException("A polygon hole must begin and end at the same point.");
            }
        });
    }

    public List<GeoPoint> getOuterBoundary() {
        return outerBoundary;
    }

    public GeoPolygon addHole(List<GeoPoint> geoPoints) {
        holeBoundaries.add(toArrayList(geoPoints));
        return this;
    }

    public List<List<GeoPoint>> getHoles() {
        return holeBoundaries;
    }

    @Override
    public int hashCode() {
        int hash = 19;
        hash = 61 * hash + outerBoundary.hashCode();
        hash = 61 * hash + holeBoundaries.hashCode();
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
        GeoPolygon other = (GeoPolygon) obj;
        return outerBoundary.equals(other.outerBoundary) && holeBoundaries.equals(other.holeBoundaries);
    }

    @Override
    public String toString() {
        return "GeoPolygon[" +
                "outerBoundary: [" + outerBoundary.stream().map(Object::toString).collect(Collectors.joining(", ")) + "]" +
                "holes: [" + holeBoundaries.stream().map(hole -> "[" + hole.stream().map(Object::toString).collect(Collectors.joining(", ")) + "]").collect(Collectors.joining(", ")) + "]" +
                "]";
    }
}
