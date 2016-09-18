package org.vertexium.accumulo.iterator.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EdgeInfoTest {
    @Test
    public void testBytes() {
        serializeDeserialize(null, null);
        serializeDeserialize("", "");
        serializeDeserialize("label", null);
        serializeDeserialize(null, "vertexId");
        serializeDeserialize("label", "vertexId");
    }

    private void serializeDeserialize(String label, String vertexId) {
        EdgeInfo edgeInfo = new EdgeInfo(label, vertexId);
        byte[] bytes = edgeInfo.getBytes();
        EdgeInfo newEdgeInfo = new EdgeInfo(bytes, 0);
        assertEquals(label, newEdgeInfo.getLabel());
        assertEquals(vertexId, newEdgeInfo.getVertexId());
    }
}