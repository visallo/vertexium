package org.vertexium.serializer.kryo;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QuickKryoVertexiumSerializerTest {
    @Test
    public void testCompress() {
        String testString = "This is a test value";
        QuickKryoVertexiumSerializer serializer = new QuickKryoVertexiumSerializer(true);
        byte[] bytes = serializer.objectToBytes(testString);
        Object str = serializer.bytesToObject(bytes);
        assertEquals(testString, str);
    }
}