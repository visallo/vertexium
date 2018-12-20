package org.vertexium.test;

import org.junit.jupiter.api.Test;
import org.vertexium.Graph;
import org.vertexium.VertexiumSerializer;
import org.vertexium.property.DefaultStreamingPropertyValue;
import org.vertexium.property.PropertyValue;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

import java.io.ByteArrayInputStream;
import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.*;

public abstract class VertexiumSerializerTestBase {
    /**
     * Tests historical {@link org.vertexium.property.PropertyValue} objects which used to have a "store: boolean" field
     */
    @Test
    public void testSerializableObjects() {
        StreamingPropertyValue spv = new DefaultStreamingPropertyValue(new ByteArrayInputStream("test".getBytes()), byte[].class, 4L)
                .searchIndex(true);

        SerializableObjects serializableObjects = new SerializableObjects();
        serializableObjects.start = "START";
        serializableObjects.propertyValue = new PropertyValue().searchIndex(true);
        serializableObjects.streamingPropertyValue = spv;
        serializableObjects.streamingPropertyValueRef = new TestStreamingPropertyValueRef(spv);
        serializableObjects.end = "END";
        byte[] bytes = getVertexiumSerializer().objectToBytes(serializableObjects);
        StringBuilder bytesString = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            if (i > 0) {
                bytesString.append(", ");
                if (i % 16 == 0) {
                    bytesString.append("\n");
                }
            }
            bytesString.append(String.format("%d", bytes[i]));
        }
        System.out.println(this.getClass().getName() + " " + bytesString.toString());

        serializableObjects = getVertexiumSerializer().bytesToObject(getSerializableObjectBytes());
        assertEquals("START", serializableObjects.start);
        assertEquals("END", serializableObjects.end);

        assertNotNull(serializableObjects.propertyValue);
        assertTrue(serializableObjects.propertyValue.isSearchIndex());

        assertNotNull(serializableObjects.streamingPropertyValue);
        assertTrue(serializableObjects.streamingPropertyValue.isSearchIndex());
        assertEquals(byte[].class, serializableObjects.streamingPropertyValue.getValueType());
        assertEquals(4L, (long) serializableObjects.streamingPropertyValue.getLength());

        assertNotNull(serializableObjects.streamingPropertyValueRef);
        assertTrue(serializableObjects.streamingPropertyValueRef.isSearchIndex());
        assertEquals(byte[].class, serializableObjects.streamingPropertyValueRef.getValueType());
    }

    protected abstract byte[] getSerializableObjectBytes();

    protected abstract VertexiumSerializer getVertexiumSerializer();

    public static class SerializableObjects implements Serializable {
        public String start;
        public PropertyValue propertyValue;
        public StreamingPropertyValue streamingPropertyValue;
        public StreamingPropertyValueRef streamingPropertyValueRef;
        public String end;
    }

    public static class TestStreamingPropertyValueRef extends StreamingPropertyValueRef {
        public TestStreamingPropertyValueRef(StreamingPropertyValue propertyValue) {
            super(propertyValue);
        }

        @Override
        public StreamingPropertyValue toStreamingPropertyValue(Graph graph, long timestamp) {
            return null;
        }
    }
}
