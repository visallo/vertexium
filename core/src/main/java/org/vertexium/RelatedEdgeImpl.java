package org.vertexium;

public class RelatedEdgeImpl implements RelatedEdge {
    private final String edgeId;
    private final String label;
    private final String outVertexId;
    private final String inVertexId;

    public RelatedEdgeImpl(String edgeId, String label, String outVertexId, String inVertexId) {
        this.edgeId = edgeId;
        this.label = label;
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
    }

    @Override
    public String getEdgeId() {
        return edgeId;
    }

    @Override
    public String getInVertexId() {
        return inVertexId;
    }

    @Override
    public String getOutVertexId() {
        return outVertexId;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String toString() {
        return "RelatedEdgeImpl{" +
            "edgeId='" + edgeId + '\'' +
            ", label='" + label + '\'' +
            ", inVertexId='" + inVertexId + '\'' +
            ", outVertexId='" + outVertexId + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof RelatedEdge)) {
            return false;
        }

        RelatedEdge that = (RelatedEdge) o;

        if (!edgeId.equals(that.getEdgeId())) {
            return false;
        }
        if (!label.equals(that.getLabel())) {
            return false;
        }
        if (!inVertexId.equals(that.getInVertexId())) {
            return false;
        }
        if (!outVertexId.equals(that.getOutVertexId())) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return edgeId.hashCode();
    }
}
