package org.vertexium.accumulo.iterator.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EdgeInfoTest {
    @Test
    public void testBytes() {
        serializeDeserialize("", "");
        serializeDeserialize("label", "vertexId");
        serializeDeserialize(new String(new char[5000]).replace('\0', 'a'), new String(new char[5000]).replace('\0', 'b'));
    }

    private void serializeDeserialize(String label, String vertexId) {
        EdgeInfo edgeInfo = new EdgeInfo(label, vertexId);
        byte[] bytes = edgeInfo.getBytes();
        EdgeInfo newEdgeInfo = new EdgeInfo(bytes, 0);
        assertEquals(label, newEdgeInfo.getLabel());
        assertEquals(vertexId, newEdgeInfo.getVertexId());
    }
}