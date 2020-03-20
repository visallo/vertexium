package org.vertexium.accumulo.iterator.model;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class IteratorEdgeInfoTest {
    @Test
    public void testBytes() throws IOException {
        serializeDeserialize("", "", new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
        serializeDeserialize(
            "label",
            "vertexId",
            new byte[]{0, 0, 0, 5, 108, 97, 98, 101, 108, 0, 0, 0, 8, 118, 101, 114, 116, 101, 120, 73, 100,}
        );

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(new byte[]{0, 0, 19, -120});
        for (int i = 0; i < 5000; i++) {
            baos.write('a');
        }
        baos.write(new byte[]{0, 0, 19, -120});
        for (int i = 0; i < 5000; i++) {
            baos.write('b');
        }
        serializeDeserialize(
            new String(new char[5000]).replace('\0', 'a'),
            new String(new char[5000]).replace('\0', 'b'),
            baos.toByteArray()
        );
    }

    private void serializeDeserialize(String label, String vertexId, byte[] bytes) {
        EdgeLabels edgeLabels = new EdgeLabels();
        IteratorEdgeInfo edgeInfo = new IteratorEdgeInfo(edgeLabels, bytes, 1);
        assertEquals(label, new String(edgeLabels.get(edgeInfo.getLabelIndex()), StandardCharsets.UTF_8));
        assertEquals(vertexId, new String(edgeInfo.getVertexIdBytes(), StandardCharsets.UTF_8));
    }
}