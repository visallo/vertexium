package org.vertexium.serializer.xstream;

import org.junit.Before;
import org.vertexium.test.VertexiumSerializerTestBase;

public class XStreamVertexiumSerializerTest extends VertexiumSerializerTestBase {
    private XStreamVertexiumSerializer vertexiumSerializer;

    @Before
    public void before() {
        vertexiumSerializer = new XStreamVertexiumSerializer();
    }

    @Override
    protected XStreamVertexiumSerializer getVertexiumSerializer() {
        return vertexiumSerializer;
    }

    @Override
    protected byte[] getPropertyValueBytes() {
        return ("<org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>\n" +
                "  <a__start>START</a__start>\n" +
                "  <b__value class=\"org.vertexium.property.PropertyValue\">\n" +
                "    <store>true</store>\n" +
                "    <searchIndex>true</searchIndex>\n" +
                "  </b__value>\n" +
                "  <z__end>END</z__end>\n" +
                "</org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>").getBytes();
    }

    @Override
    protected byte[] getStreamingPropertyValueBytes() {
        return ("<org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>\n" +
                "  <a__start>START</a__start>\n" +
                "  <b__value class=\"org.vertexium.property.DefaultStreamingPropertyValue\">\n" +
                "    <store>true</store>\n" +
                "    <searchIndex>true</searchIndex>\n" +
                "    <valueType>[B</valueType>\n" +
                "    <length>4</length>\n" +
                "  </b__value>\n" +
                "  <z__end>END</z__end>\n" +
                "</org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>").getBytes();
    }

    @Override
    protected byte[] getStreamingPropertyValueRefBytes() {
        return ("<org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>\n" +
                "  <a__start>START</a__start>\n" +
                "  <b__value class=\"org.vertexium.test.VertexiumSerializerTestBase$TestStreamingPropertyValueRef\">\n" +
                "    <valueType>[B</valueType>\n" +
                "    <searchIndex>true</searchIndex>\n" +
                "    <store>true</store>\n" +
                "  </b__value>\n" +
                "  <z__end>END</z__end>\n" +
                "</org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>").getBytes();
    }

    @Override
    protected byte[] getGeoPointBytes() {
        return ("<org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>\n" +
                "  <a__start>START</a__start>\n" +
                "  <b__value class=\"org.vertexium.type.GeoPoint\">\n" +
                "    <description>Geo point with description</description>\n" +
                "    <latitude>12.123</latitude>\n" +
                "    <longitude>23.234</longitude>\n" +
                "    <altitude>34.345</altitude>\n" +
                "  </b__value>\n" +
                "  <z__end>END</z__end>\n" +
                "</org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>").getBytes();
    }

    @Override
    protected byte[] getGeoPointWithAccuracyBytes() {
        return ("<org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>\n" +
                "  <a__start>START</a__start>\n" +
                "  <b__value class=\"org.vertexium.type.GeoPoint\">\n" +
                "    <description>Geo point with accuracy and description</description>\n" +
                "    <latitude>12.123</latitude>\n" +
                "    <longitude>23.234</longitude>\n" +
                "    <altitude>34.345</altitude>\n" +
                "    <accuracy>45.456</accuracy>\n" +
                "  </b__value>\n" +
                "  <z__end>END</z__end>\n" +
                "</org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>").getBytes();
    }

    @Override
    protected byte[] getGeoCircleBytes() {
        return ("<org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>\n" +
                "  <a__start>START</a__start>\n" +
                "  <b__value class=\"org.vertexium.type.GeoCircle\">\n" +
                "    <description>Geo circle with description</description>\n" +
                "    <latitude>12.123</latitude>\n" +
                "    <longitude>23.234</longitude>\n" +
                "    <radius>34.345</radius>\n" +
                "  </b__value>\n" +
                "  <z__end>END</z__end>\n" +
                "</org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>").getBytes();
    }

    @Override
    protected byte[] getGeoRectBytes() {
        return ("<org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>\n" +
                "  <a__start>START</a__start>\n" +
                "  <b__value class=\"org.vertexium.type.GeoRect\">\n" +
                "    <description>Geo rect with description</description>\n" +
                "    <northWest>\n" +
                "      <latitude>12.123</latitude>\n" +
                "      <longitude>23.234</longitude>\n" +
                "    </northWest>\n" +
                "    <southEast>\n" +
                "      <latitude>34.345</latitude>\n" +
                "      <longitude>45.456</longitude>\n" +
                "    </southEast>\n" +
                "  </b__value>\n" +
                "  <z__end>END</z__end>\n" +
                "</org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>").getBytes();
    }

    @Override
    protected byte[] getGeoLineBytes() {
        return ("<org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>\n" +
                "  <a__start>START</a__start>\n" +
                "  <b__value class=\"org.vertexium.type.GeoLine\">\n" +
                "    <description>Geo line with description</description>\n" +
                "    <geoPoints>\n" +
                "      <org.vertexium.type.GeoPoint>\n" +
                "        <latitude>12.123</latitude>\n" +
                "        <longitude>23.234</longitude>\n" +
                "      </org.vertexium.type.GeoPoint>\n" +
                "      <org.vertexium.type.GeoPoint>\n" +
                "        <latitude>34.345</latitude>\n" +
                "        <longitude>45.456</longitude>\n" +
                "      </org.vertexium.type.GeoPoint>\n" +
                "    </geoPoints>\n" +
                "  </b__value>\n" +
                "  <z__end>END</z__end>\n" +
                "</org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>").getBytes();
    }

    @Override
    protected byte[] getGeoHashBytes() {
        return ("<org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>\n" +
                "  <a__start>START</a__start>\n" +
                "  <b__value class=\"org.vertexium.type.GeoHash\">\n" +
                "    <description>Geo hash with description</description>\n" +
                "    <hash>sd0sbwymx6yp</hash>\n" +
                "  </b__value>\n" +
                "  <z__end>END</z__end>\n" +
                "</org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>").getBytes();
    }

    @Override
    protected byte[] getGeoCollectionBytes() {
        return ("<org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>\n" +
                "  <a__start>START</a__start>\n" +
                "  <b__value class=\"org.vertexium.type.GeoCollection\">\n" +
                "    <description>Geo collection with description</description>\n" +
                "    <geoShapes>\n" +
                "      <org.vertexium.type.GeoPoint>\n" +
                "        <latitude>12.123</latitude>\n" +
                "        <longitude>23.234</longitude>\n" +
                "      </org.vertexium.type.GeoPoint>\n" +
                "      <org.vertexium.type.GeoPoint>\n" +
                "        <latitude>34.345</latitude>\n" +
                "        <longitude>45.456</longitude>\n" +
                "      </org.vertexium.type.GeoPoint>\n" +
                "    </geoShapes>\n" +
                "  </b__value>\n" +
                "  <z__end>END</z__end>\n" +
                "</org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>").getBytes();
    }

    @Override
    protected byte[] getGeoPolygonBytes() {
        return ("<org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>\n" +
                "  <a__start>START</a__start>\n" +
                "  <b__value class=\"org.vertexium.type.GeoPolygon\">\n" +
                "    <description>Geo collection with description</description>\n" +
                "    <outerBoundary>\n" +
                "      <org.vertexium.type.GeoPoint>\n" +
                "        <latitude>12.123</latitude>\n" +
                "        <longitude>23.234</longitude>\n" +
                "      </org.vertexium.type.GeoPoint>\n" +
                "      <org.vertexium.type.GeoPoint>\n" +
                "        <latitude>34.345</latitude>\n" +
                "        <longitude>45.456</longitude>\n" +
                "      </org.vertexium.type.GeoPoint>\n" +
                "      <org.vertexium.type.GeoPoint>\n" +
                "        <latitude>56.567</latitude>\n" +
                "        <longitude>67.678</longitude>\n" +
                "      </org.vertexium.type.GeoPoint>\n" +
                "      <org.vertexium.type.GeoPoint>\n" +
                "        <latitude>12.123</latitude>\n" +
                "        <longitude>23.234</longitude>\n" +
                "      </org.vertexium.type.GeoPoint>\n" +
                "    </outerBoundary>\n" +
                "    <holeBoundaries>\n" +
                "      <list>\n" +
                "        <org.vertexium.type.GeoPoint>\n" +
                "          <latitude>78.789</latitude>\n" +
                "          <longitude>89.89</longitude>\n" +
                "        </org.vertexium.type.GeoPoint>\n" +
                "        <org.vertexium.type.GeoPoint>\n" +
                "          <latitude>65.654</latitude>\n" +
                "          <longitude>54.543</longitude>\n" +
                "        </org.vertexium.type.GeoPoint>\n" +
                "        <org.vertexium.type.GeoPoint>\n" +
                "          <latitude>43.432</latitude>\n" +
                "          <longitude>32.321</longitude>\n" +
                "        </org.vertexium.type.GeoPoint>\n" +
                "        <org.vertexium.type.GeoPoint>\n" +
                "          <latitude>78.789</latitude>\n" +
                "          <longitude>89.89</longitude>\n" +
                "        </org.vertexium.type.GeoPoint>\n" +
                "      </list>\n" +
                "      <list>\n" +
                "        <org.vertexium.type.GeoPoint>\n" +
                "          <latitude>21.21</latitude>\n" +
                "          <longitude>10.109</longitude>\n" +
                "        </org.vertexium.type.GeoPoint>\n" +
                "        <org.vertexium.type.GeoPoint>\n" +
                "          <latitude>87.876</latitude>\n" +
                "          <longitude>76.765</longitude>\n" +
                "        </org.vertexium.type.GeoPoint>\n" +
                "        <org.vertexium.type.GeoPoint>\n" +
                "          <latitude>65.654</latitude>\n" +
                "          <longitude>54.543</longitude>\n" +
                "        </org.vertexium.type.GeoPoint>\n" +
                "        <org.vertexium.type.GeoPoint>\n" +
                "          <latitude>21.21</latitude>\n" +
                "          <longitude>10.109</longitude>\n" +
                "        </org.vertexium.type.GeoPoint>\n" +
                "      </list>\n" +
                "    </holeBoundaries>\n" +
                "  </b__value>\n" +
                "  <z__end>END</z__end>\n" +
                "</org.vertexium.test.VertexiumSerializerTestBase_-SerializableObject>").getBytes();
    }

    @Override
    protected void printSerializedObject(SerializableObject serializableObject) {
        String str = new String(getVertexiumSerializer().objectToBytes(serializableObject));
        System.out.println(this.getClass().getName() + " " + str);
    }
}