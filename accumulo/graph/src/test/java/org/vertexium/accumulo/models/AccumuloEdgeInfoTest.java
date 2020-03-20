package org.vertexium.accumulo.models;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class AccumuloEdgeInfoTest {
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
        AccumuloEdgeInfo edgeInfo = AccumuloEdgeInfo.parse(bytes, 1);
        assertEquals(label, edgeInfo.getLabel());
        assertEquals(vertexId, edgeInfo.getVertexId());
    }
}