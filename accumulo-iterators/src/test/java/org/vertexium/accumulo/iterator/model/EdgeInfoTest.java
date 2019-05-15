package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EdgeInfoTest {
    @Test
    public void testBytes() {
        serializeDeserialize("", "");
        serializeDeserialize("label", "vertexId");
        serializeDeserialize(new String(new char[5000]).replace('\0', 'a'), new String(new char[5000]).replace('\0', 'b'));
    }

    private void serializeDeserialize(String label, String vertexId) {
        EdgeInfo edgeInfo = new EdgeInfo(label, vertexId, new Text("vis1"));
        byte[] bytes = edgeInfo.getBytes();
        EdgeInfo newEdgeInfo = new EdgeInfo(bytes, new Text("vis1"), 0);
        assertEquals(label, newEdgeInfo.getLabel());
        assertEquals(vertexId, newEdgeInfo.getVertexId());
    }
}