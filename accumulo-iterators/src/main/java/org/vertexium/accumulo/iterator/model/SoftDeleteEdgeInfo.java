package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;

public class SoftDeleteEdgeInfo {
    private final Text edgeId;
    private final long timestamp;

    public SoftDeleteEdgeInfo(Text edgeId, long timestamp) {
        this.edgeId = edgeId;
        this.timestamp = timestamp;
    }

    public Text getEdgeId() {
        return edgeId;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
