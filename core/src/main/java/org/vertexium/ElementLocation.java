package org.vertexium;

public interface ElementLocation extends ElementId {
    /**
     * the visibility of the element.
     */
    Visibility getVisibility();

    static EdgeElementLocation edge(
        String id,
        Visibility visibility,
        String label,
        String outVertexId,
        String inVertexId
    ) {
        return new DefaultEdgeElementLocation(
            id,
            visibility,
            label,
            outVertexId,
            inVertexId
        );
    }

    static VertexElementLocation vertex(String id, Visibility visibility) {
        return new DefaultVertexElementLocation(id, visibility);
    }
}
