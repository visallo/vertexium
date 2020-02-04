package org.vertexium.type;

import org.vertexium.util.GeoUtils;

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
    public void validate() {
        GeoUtils.toJtsPolygon(this.outerBoundary, this.holeBoundaries, false);
    }

    public static GeoShape createLenient(List<GeoPoint> outerBoundary) {
        return createLenient(outerBoundary, null, null);
    }

    public static GeoShape createLenient(List<GeoPoint> outerBoundary, String description) {
        return createLenient(outerBoundary, null, description);
    }

    public static GeoShape createLenient(List<GeoPoint> outerBoundary, List<List<GeoPoint>> holeBoundaries) {
        return createLenient(outerBoundary, holeBoundaries, null);
    }

    /**
     * Try to make a polygon with the coordinates provided. If that fails validation, attempt to automatically correct the errors.
     */
    public static GeoShape createLenient(List<GeoPoint> outerBoundary, List<List<GeoPoint>> holeBoundaries, String description) {
        try {
            return new GeoPolygon(outerBoundary, holeBoundaries, description);
        } catch (VertexiumInvalidShapeException ve) {
            return GeoUtils.toGeoShape(GeoUtils.toJtsPolygon(outerBoundary, holeBoundaries, true), description);
        }
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
