package org.vertexium.test;

import org.junit.Test;
import org.vertexium.Graph;
import org.vertexium.VertexiumSerializer;
import org.vertexium.property.DefaultStreamingPropertyValue;
import org.vertexium.property.PropertyValue;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.type.*;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public abstract class VertexiumSerializerTestBase {
    /**
     * Tests historical {@link org.vertexium.property.PropertyValue} objects which used to have a "store: boolean" field
     */
    @Test
    public void testPropertyValue() {
        PropertyValue propertyValue = new PropertyValue().searchIndex(true);

        testValue(
                propertyValue,
                getPropertyValueBytes(),
                (givenValue, deserializedValue) -> {
                    assertNotNull(deserializedValue);
                    assertTrue(deserializedValue.isSearchIndex());
                }
        );
    }

    protected abstract byte[] getPropertyValueBytes();

    @Test
    public void testStreamingPropertyValue() {
        StreamingPropertyValue spv = new DefaultStreamingPropertyValue(new ByteArrayInputStream("test".getBytes()), byte[].class, 4L)
                .searchIndex(true);

        testValue(
                spv,
                getStreamingPropertyValueBytes(),
                (givenValue, deserializedValue) -> {
                    assertNotNull(deserializedValue);
                    assertTrue(deserializedValue.isSearchIndex());
                    assertEquals(byte[].class, deserializedValue.getValueType());
                    assertEquals(4L, (long) deserializedValue.getLength());
                }
        );
    }

    protected abstract byte[] getStreamingPropertyValueBytes();

    @Test
    public void testStreamingPropertyValueRef() {
        StreamingPropertyValue spv = new DefaultStreamingPropertyValue(new ByteArrayInputStream("test".getBytes()), byte[].class, 4L)
                .searchIndex(true);
        StreamingPropertyValueRef streamingPropertyValueRef = new TestStreamingPropertyValueRef(spv);

        testValue(
                streamingPropertyValueRef,
                getStreamingPropertyValueRefBytes(),
                (givenValue, deserializedValue) -> {
                    assertNotNull(deserializedValue);
                    assertTrue(deserializedValue.isSearchIndex());
                    assertEquals(byte[].class, deserializedValue.getValueType());
                }
        );
    }

    protected abstract byte[] getStreamingPropertyValueRefBytes();

    @Test
    public void testGeoPoint() {
        GeoPoint geoPoint = new GeoPoint(12.123, 23.234, 34.345, "Geo point with description");

        testValue(
                geoPoint,
                getGeoPointBytes(),
                (givenValue, deserializedValue) -> {
                    assertNotNull(deserializedValue);
                    assertEquals(givenValue.getLatitude(), deserializedValue.getLatitude(), 0.001);
                    assertEquals(givenValue.getLongitude(), deserializedValue.getLongitude(), 0.001);
                    assertEquals(givenValue.getAltitude(), deserializedValue.getAltitude(), 0.001);
                    assertEquals(givenValue.getDescription(), deserializedValue.getDescription());
                }
        );
    }

    protected abstract byte[] getGeoPointBytes();

    @Test
    public void testGeoPointWithAccuracy() {
        GeoPoint geoPoint = new GeoPoint(12.123, 23.234, 34.345, 45.456, "Geo point with accuracy and description");

        testValue(
                geoPoint,
                getGeoPointWithAccuracyBytes(),
                (givenValue, deserializedValue) -> {
                    assertNotNull(deserializedValue);
                    assertEquals(givenValue.getLatitude(), deserializedValue.getLatitude(), 0.001);
                    assertEquals(givenValue.getLongitude(), deserializedValue.getLongitude(), 0.001);
                    assertEquals(givenValue.getAltitude(), deserializedValue.getAltitude(), 0.001);
                    assertEquals(givenValue.getAccuracy(), deserializedValue.getAccuracy(), 0.001);
                    assertEquals(givenValue.getDescription(), deserializedValue.getDescription());
                }
        );
    }

    protected abstract byte[] getGeoPointWithAccuracyBytes();

    @Test
    public void testGeoCircle() {
        GeoCircle geoCircle = new GeoCircle(12.123, 23.234, 34.345, "Geo circle with description");

        testValue(
                geoCircle,
                getGeoCircleBytes(),
                (givenValue, deserializedValue) -> {
                    assertNotNull(deserializedValue);
                    assertEquals(givenValue.getLatitude(), deserializedValue.getLatitude(), 0.001);
                    assertEquals(givenValue.getLongitude(), deserializedValue.getLongitude(), 0.001);
                    assertEquals(givenValue.getRadius(), deserializedValue.getRadius(), 0.001);
                    assertEquals(givenValue.getDescription(), deserializedValue.getDescription());
                }
        );
    }

    protected abstract byte[] getGeoCircleBytes();

    @Test
    public void testGeoRect() {
        GeoRect geoRect = new GeoRect(
                new GeoPoint(12.123, 23.234),
                new GeoPoint(34.345, 45.456),
                "Geo rect with description"
        );

        testValue(
                geoRect,
                getGeoRectBytes(),
                (givenValue, deserializedValue) -> {
                    assertNotNull(deserializedValue);
                    assertEquals(givenValue.getNorthWest(), deserializedValue.getNorthWest());
                    assertEquals(givenValue.getSouthEast(), deserializedValue.getSouthEast());
                    assertEquals(givenValue.getDescription(), deserializedValue.getDescription());
                }
        );
    }

    protected abstract byte[] getGeoRectBytes();

    @Test
    public void testGeoLine() {
        GeoLine geoLine = new GeoLine(
                new GeoPoint(12.123, 23.234),
                new GeoPoint(34.345, 45.456),
                "Geo line with description"
        );

        testValue(
                geoLine,
                getGeoLineBytes(),
                (givenValue, deserializedValue) -> {
                    assertNotNull(deserializedValue);
                    assertEquals(givenValue.getGeoPoints().size(), deserializedValue.getGeoPoints().size());
                    assertEquals(givenValue.getGeoPoints().get(0), deserializedValue.getGeoPoints().get(0));
                    assertEquals(givenValue.getGeoPoints().get(1), deserializedValue.getGeoPoints().get(1));
                    assertEquals(givenValue.getDescription(), deserializedValue.getDescription());
                }
        );
    }

    protected abstract byte[] getGeoLineBytes();

    @Test
    public void testGeoHash() {
        GeoHash geoLine = new GeoHash(
                12.123,
                23.234,
                12,
                "Geo hash with description"
        );

        testValue(
                geoLine,
                getGeoHashBytes(),
                (givenValue, deserializedValue) -> {
                    assertNotNull(deserializedValue);
                    assertEquals(givenValue.getHash(), deserializedValue.getHash());
                    assertEquals(givenValue.getDescription(), deserializedValue.getDescription());
                }
        );
    }

    protected abstract byte[] getGeoHashBytes();

    @Test
    public void testGeoCollection() {
        GeoCollection geoCollection = new GeoCollection(
                Arrays.asList(
                        new GeoPoint(12.123, 23.234),
                        new GeoPoint(34.345, 45.456)
                ),
                "Geo collection with description"
        );

        testValue(
                geoCollection,
                getGeoCollectionBytes(),
                (givenValue, deserializedValue) -> {
                    assertNotNull(deserializedValue);
                    assertEquals(givenValue.getGeoShapes().size(), deserializedValue.getGeoShapes().size());
                    assertEquals(givenValue.getGeoShapes().get(0), deserializedValue.getGeoShapes().get(0));
                    assertEquals(givenValue.getGeoShapes().get(1), deserializedValue.getGeoShapes().get(1));
                    assertEquals(givenValue.getDescription(), deserializedValue.getDescription());
                }
        );
    }

    protected abstract byte[] getGeoCollectionBytes();

    @Test
    public void testGeoPolygon() {
        GeoPolygon geoPolygon = new GeoPolygon(
                Arrays.asList(
                        new GeoPoint(12.123, 23.234),
                        new GeoPoint(34.345, 45.456),
                        new GeoPoint(56.567, 67.678),
                        new GeoPoint(12.123, 23.234)
                ),
                Arrays.asList(
                        Arrays.asList(
                                new GeoPoint(78.789, 89.890),
                                new GeoPoint(65.654, 54.543),
                                new GeoPoint(43.432, 32.321),
                                new GeoPoint(78.789, 89.890)
                        ),
                        Arrays.asList(
                                new GeoPoint(21.210, 10.109),
                                new GeoPoint(87.876, 76.765),
                                new GeoPoint(65.654, 54.543),
                                new GeoPoint(21.210, 10.109)
                        )
                ),
                "Geo collection with description"
        );

        testValue(
                geoPolygon,
                getGeoPolygonBytes(),
                (givenValue, deserializedValue) -> {
                    assertNotNull(deserializedValue);

                    assertEquals(givenValue.getOuterBoundary().size(), deserializedValue.getOuterBoundary().size());
                    for (int i = 0; i < givenValue.getOuterBoundary().size(); i++) {
                        assertEquals(givenValue.getOuterBoundary().get(i), deserializedValue.getOuterBoundary().get(i));
                    }

                    assertEquals(givenValue.getHoles().size(), deserializedValue.getHoles().size());
                    for (int i = 0; i < givenValue.getHoles().size(); i++) {
                        List<GeoPoint> givenHole = givenValue.getHoles().get(i);
                        List<GeoPoint> deserializedHole = deserializedValue.getHoles().get(i);
                        for (int j = 0; j < givenHole.size(); j++) {
                            assertEquals(givenHole.get(j), deserializedHole.get(j));
                        }
                    }

                    assertEquals(givenValue.getDescription(), deserializedValue.getDescription());
                }
        );
    }

    protected abstract byte[] getGeoPolygonBytes();

    protected <T> void testValue(T value, byte[] bytes, TestValueCallback<T> fn) {
        SerializableObject serializableObject = new SerializableObject<T>();
        serializableObject.a_start = "START";
        serializableObject.b_value = value;
        serializableObject.z_end = "END";

        printSerializedObject(serializableObject);

        SerializableObject<T> deserializableObject = getVertexiumSerializer().bytesToObject(bytes);
        assertEquals("START", serializableObject.a_start);
        assertEquals("END", serializableObject.z_end);

        fn.apply(value, deserializableObject.b_value);
    }

    protected interface TestValueCallback<T> {
        void apply(T givenValue, T deserializedValue);
    }

    protected void printSerializedObject(SerializableObject serializableObject) {
        byte[] bytes = getVertexiumSerializer().objectToBytes(serializableObject);
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
    }

    protected abstract VertexiumSerializer getVertexiumSerializer();

    public static class SerializableObject<T> implements Serializable {
        public String a_start;
        public T b_value;
        public String z_end;
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
