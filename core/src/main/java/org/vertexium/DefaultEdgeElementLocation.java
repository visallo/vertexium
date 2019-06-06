package org.vertexium;

class DefaultEdgeElementLocation extends DefaultElementLocation implements EdgeElementLocation {
    private final String label;
    private final String outVertexId;
    private final String inVertexId;

    DefaultEdgeElementLocation(
        String id,
        Visibility visibility,
        String label,
        String outVertexId,
        String inVertexId
    ) {
        super(ElementType.EDGE, id, visibility);
        this.label = label;
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
    }

    @Override
    public String getVertexId(Direction direction) {
        switch (direction) {
            case OUT:
                return getOutVertexId();
            case IN:
                return getInVertexId();
            default:
                throw new VertexiumException("Direction not handled: " + direction);
        }
    }

    @Override
    public String getLabel() {
        return label;
    }

    public String getOutVertexId() {
        return outVertexId;
    }

    public String getInVertexId() {
        return inVertexId;
    }
}
