package org.vertexium.accumulo.iterator.model;

public class SoftDeleteEdgeInfo {
    private final String edgeId;
    private final long timestamp;

    public SoftDeleteEdgeInfo(String edgeId, long timestamp) {
        this.edgeId = edgeId;
        this.timestamp = timestamp;
    }

    public String getEdgeId() {
        return edgeId;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
