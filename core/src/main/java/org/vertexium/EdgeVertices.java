package org.vertexium;

public class EdgeVertices {
    private final Vertex outVertex;
    private final Vertex inVertex;

    public EdgeVertices(Vertex outVertex, Vertex inVertex) {
        this.outVertex = outVertex;
        this.inVertex = inVertex;
    }

    public Vertex getOutVertex() {
        return outVertex;
    }

    public Vertex getInVertex() {
        return inVertex;
    }

    @Override
    public String toString() {
        return "EdgeVertices{" +
            "outVertex=" + outVertex +
            ", inVertex=" + inVertex +
            '}';
    }
}
