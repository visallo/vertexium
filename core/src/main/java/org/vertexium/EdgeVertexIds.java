package org.vertexium;

public class EdgeVertexIds implements Comparable<String> {
    private final String outVertexId;
    private final String inVertexId;

    public EdgeVertexIds(String outVertexId, String inVertexId) {
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
    }

    public String getOutVertexId() {
        return outVertexId;
    }

    public String getInVertexId() {
        return inVertexId;
    }

    @Override
    public int compareTo(String id) {
        if (getOutVertexId().equals(id) || getInVertexId().equals(id)) {
            return 0;
        }
        return 1;
    }
}
