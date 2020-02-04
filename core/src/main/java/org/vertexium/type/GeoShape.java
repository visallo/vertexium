package org.vertexium.type;

public interface GeoShape {
    /**
     * Calculate whether the given GeoShape intersects with the geometry of this object.
     *
     * @param geoShape The other shape
     * @return True if any points of the provided shape are within the geometry of this shape or vice-versa.
     */
    boolean intersects(GeoShape geoShape);

    /**
     * Calculate whether this shape is completely within the geometry of the given object.
     *
     * @param geoShape The other shape
     * @return True if this shape is completely within the geometry of the provided shape.
     */
    boolean within(GeoShape geoShape);

    /**
     * Throw a VertexiumInvalidShapeException if the requirements for this shape are not met.
     */
    void validate();

    String getDescription();
}
