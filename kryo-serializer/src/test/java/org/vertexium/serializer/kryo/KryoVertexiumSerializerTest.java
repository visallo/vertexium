package org.vertexium.serializer.kryo;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumSerializer;
import org.vertexium.test.VertexiumSerializerTestBase;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class KryoVertexiumSerializerTest extends VertexiumSerializerTestBase {
    private final boolean compress;
    private int iterations;
    private boolean shouldAssertTiming;
    private GraphConfiguration graphConfiguration;
    private KryoVertexiumSerializer vertexiumSerializer;

    @Parameterized.Parameters(name = "compress={0}")
    public static Iterable<Object[]> initialVisibilitySources() {
        return Arrays.asList(new Object[][]{
            {true}, {false}
        });
    }

    public KryoVertexiumSerializerTest(boolean compress) {
        this.compress = compress;
    }

    @Before
    public void before() {
        System.gc();

        Map<String, Object> config = new HashMap<>();
        config.put(QuickKryoVertexiumSerializer.CONFIG_COMPRESS, compress);
        shouldAssertTiming = !compress;
        iterations = compress ? 100 : 100000;
        graphConfiguration = new GraphConfiguration(config);
        vertexiumSerializer = new KryoVertexiumSerializer();
    }

    @Override
    protected byte[] getPropertyValueBytes() {
        return new byte[]{
            1, 0, -62, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
            109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
            83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
            115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
            98, 106, 101, 99, 116, 1, 83, 84, 65, 82, -44, 1, 1, 111, 114, 103,
            46, 118, 101, 114, 116, 101, 120, 105, 117, 109, 46, 112, 114, 111, 112, 101,
            114, 116, 121, 46, 80, 114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117,
            -27, 1, 1, 1, 69, 78, -60
        };
    }

    @Override
    protected byte[] getStreamingPropertyValueBytes() {
        return new byte[]{
            1, 0, -62, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
            109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
            83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
            115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
            98, 106, 101, 99, 116, 1, 83, 84, 65, 82, -44, 1, 1, 111, 114, 103,
            46, 118, 101, 114, 116, 101, 120, 105, 117, 109, 46, 112, 114, 111, 112, 101,
            114, 116, 121, 46, 68, 101, 102, 97, 117, 108, 116, 83, 116, 114, 101, 97,
            109, 105, 110, 103, 80, 114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117,
            -27, 1, 1, 4, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2,
            91, -62, 0, 69, 78, -60
        };
    }

    @Override
    protected byte[] getStreamingPropertyValueRefBytes() {
        return new byte[]{
            1, 0, -62, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
            109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
            83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
            115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
            98, 106, 101, 99, 116, 1, 83, 84, 65, 82, -44, 1, 1, -51, 1, 111,
            114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117, 109, 46, 116, 101, 115,
            116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109, 83, 101, 114, 105, 97,
            108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97, 115, 101, 36, 84, 101,
            115, 116, 83, 116, 114, 101, 97, 109, 105, 110, 103, 80, 114, 111, 112, 101,
            114, 116, 121, 86, 97, 108, 117, 101, 82, 101, 102, 1, 1, 1, 91, -62,
            69, 78, -60
        };
    }

    @Override
    protected byte[] getGeoPointBytes() {
        return new byte[]{
            1, 0, -62, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
            109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
            83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
            115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
            98, 106, 101, 99, 116, 1, 83, 84, 65, 82, -44, -21, 7, 1, 1, 92,
            -113, -62, -11, 40, 44, 65, 64, 71, 101, 111, 32, 112, 111, 105, 110, 116,
            32, 119, 105, 116, 104, 32, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111,
            -18, -27, -48, 34, -37, -7, 62, 40, 64, -106, 67, -117, 108, -25, 59, 55,
            64, 69, 78, -60
        };
    }

    @Override
    protected byte[] getGeoPointWithAccuracyBytes() {
        return new byte[]{
            1, 0, -62, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
            109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
            83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
            115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
            98, 106, 101, 99, 116, 1, 83, 84, 65, 82, -44, -9, 7, 1, 1, -18,
            124, 63, 53, 94, -70, 70, 64, 1, 92, -113, -62, -11, 40, 44, 65, 64,
            71, 101, 111, 32, 112, 111, 105, 110, 116, 32, 119, 105, 116, 104, 32, 97,
            99, 99, 117, 114, 97, 99, 121, 32, 97, 110, 100, 32, 100, 101, 115, 99,
            114, 105, 112, 116, 105, 111, -18, -27, -48, 34, -37, -7, 62, 40, 64, -106,
            67, -117, 108, -25, 59, 55, 64, 69, 78, -60
        };
    }

    @Override
    protected byte[] getGeoCircleBytes() {
        return new byte[]{
            1, 0, -62, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
            109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
            83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
            115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
            98, 106, 101, 99, 116, 1, 83, 84, 65, 82, -44, -15, 7, 1, 71, 101,
            111, 32, 99, 105, 114, 99, 108, 101, 32, 119, 105, 116, 104, 32, 100, 101,
            115, 99, 114, 105, 112, 116, 105, 111, -18, -27, -48, 34, -37, -7, 62, 40,
            64, -106, 67, -117, 108, -25, 59, 55, 64, 92, -113, -62, -11, 40, 44, 65,
            64, 69, 78, -60
        };
    }

    @Override
    protected byte[] getGeoRectBytes() {
        return new byte[]{
            1, 0, -62, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
            109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
            83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
            115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
            98, 106, 101, 99, 116, 1, 83, 84, 65, 82, -44, -16, 7, 1, 71, 101,
            111, 32, 114, 101, 99, 116, 32, 119, 105, 116, 104, 32, 100, 101, 115, 99,
            114, 105, 112, 116, 105, 111, -18, -21, 7, 1, 0, -128, -27, -48, 34, -37,
            -7, 62, 40, 64, -106, 67, -117, 108, -25, 59, 55, 64, -21, 7, 1, 0,
            -128, 92, -113, -62, -11, 40, 44, 65, 64, -18, 124, 63, 53, 94, -70, 70,
            64, 69, 78, -60
        };
    }

    @Override
    protected byte[] getGeoLineBytes() {
        return new byte[]{
            1, 0, -62, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
            109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
            83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
            115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
            98, 106, 101, 99, 116, 1, 83, 84, 65, 82, -44, -12, 7, 1, 71, 101,
            111, 32, 108, 105, 110, 101, 32, 119, 105, 116, 104, 32, 100, 101, 115, 99,
            114, 105, 112, 116, 105, 111, -18, 1, 1, 106, 97, 118, 97, 46, 117, 116,
            105, 108, 46, 65, 114, 114, 97, 121, 76, 105, 115, -12, 1, 2, -21, 7,
            1, 0, -128, -27, -48, 34, -37, -7, 62, 40, 64, -106, 67, -117, 108, -25,
            59, 55, 64, -21, 7, 1, 0, -128, 92, -113, -62, -11, 40, 44, 65, 64,
            -18, 124, 63, 53, 94, -70, 70, 64, 69, 78, -60
        };
    }

    @Override
    protected byte[] getGeoHashBytes() {
        return new byte[]{
            1, 0, -62, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
            109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
            83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
            115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
            98, 106, 101, 99, 116, 1, 83, 84, 65, 82, -44, 1, 1, 111, 114, 103,
            46, 118, 101, 114, 116, 101, 120, 105, 117, 109, 46, 116, 121, 112, 101, 46,
            71, 101, 111, 72, 97, 115, -24, 1, 71, 101, 111, 32, 104, 97, 115, 104,
            32, 119, 105, 116, 104, 32, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111,
            -18, 115, 100, 48, 115, 98, 119, 121, 109, 120, 54, 121, -16, 69, 78, -60
        };
    }

    @Override
    protected byte[] getGeoCollectionBytes() {
        return new byte[]{
            1, 0, -62, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
            109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
            83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
            115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
            98, 106, 101, 99, 116, 1, 83, 84, 65, 82, -44, -13, 7, 1, 71, 101,
            111, 32, 99, 111, 108, 108, 101, 99, 116, 105, 111, 110, 32, 119, 105, 116,
            104, 32, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111, -18, 1, 1, 106,
            97, 118, 97, 46, 117, 116, 105, 108, 46, 65, 114, 114, 97, 121, 76, 105,
            115, -12, 1, 2, -21, 7, 1, 0, -128, -27, -48, 34, -37, -7, 62, 40,
            64, -106, 67, -117, 108, -25, 59, 55, 64, -21, 7, 1, 0, -128, 92, -113,
            -62, -11, 40, 44, 65, 64, -18, 124, 63, 53, 94, -70, 70, 64, 69, 78,
            -60
        };
    }

    @Override
    protected byte[] getGeoPolygonBytes() {
        return new byte[]{
            1, 0, -62, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
            109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
            83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
            115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
            98, 106, 101, 99, 116, 1, 83, 84, 65, 82, -44, -11, 7, 1, 71, 101,
            111, 80, 111, 108, 121, 103, 111, 110, 32, 116, 111, 32, 116, 101, 115, 116,
            32, 115, 101, 114, 105, 97, 108, 105, 122, 97, 116, 105, 111, -18, 1, 1,
            106, 97, 118, 97, 46, 117, 116, 105, 108, 46, 65, 114, 114, 97, 121, 76,
            105, 115, -12, 1, 1, 1, 1, 1, 4, -9, 7, 1, 0, 0, -128, 125,
            63, 53, 94, -70, -71, 70, 64, -80, 114, 104, -111, -19, 76, 78, 64, -9,
            7, 1, 0, 0, -128, 55, -119, 65, 96, -27, 48, 67, 64, -80, 114, 104,
            -111, -19, -84, 81, 64, -9, 7, 1, 0, 0, -128, 119, -66, -97, 26, 47,
            -83, 65, 64, -14, -46, 77, 98, 16, 72, 76, 64, -9, 7, 1, 0, 0,
            -128, 125, 63, 53, 94, -70, -71, 70, 64, -80, 114, 104, -111, -19, 76, 78,
            64, 1, 1, 1, 4, -9, 7, 1, 0, 0, -128, -27, -48, 34, -37, -7,
            62, 40, 64, -106, 67, -117, 108, -25, 59, 55, 64, -9, 7, 1, 0, 0,
            -128, 92, -113, -62, -11, 40, 44, 65, 64, 119, -66, -97, 26, 47, 29, 84,
            64, -9, 7, 1, 0, 0, -128, 127, 106, -68, 116, -109, 72, 76, 64, 8,
            -84, 28, 90, 100, -21, 80, 64, -9, 7, 1, 0, 0, -128, -27, -48, 34,
            -37, -7, 62, 40, 64, -106, 67, -117, 108, -25, 59, 55, 64, 69, 78, -60
        };
    }

    @Override
    protected byte[] getLegacyGeoPolygonBytes() {
        return new byte[]{
            1, 0, -62, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
            109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
            83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
            115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
            98, 106, 101, 99, 116, 1, 83, 84, 65, 82, -44, -11, 7, 1, 71, 101,
            111, 32, 99, 111, 108, 108, 101, 99, 116, 105, 111, 110, 32, 119, 105, 116,
            104, 32, 100, 101, 115, 99, 114, 105, 112, 116, 105, 111, -18, 1, 1, 106,
            97, 118, 97, 46, 117, 116, 105, 108, 46, 65, 114, 114, 97, 121, 76, 105,
            115, -12, 1, 2, 1, 1, 1, 4, -21, 7, 1, 0, -128, -47, 34, -37,
            -7, 126, -78, 83, 64, 41, 92, -113, -62, -11, 120, 86, 64, -21, 7, 1,
            0, -128, 96, -27, -48, 34, -37, 105, 80, 64, 47, -35, 36, 6, -127, 69,
            75, 64, -21, 7, 1, 0, -128, -98, -17, -89, -58, 75, -73, 69, 64, 12,
            2, 43, -121, 22, 41, 64, 64, -21, 7, 1, 0, -128, -47, 34, -37, -7,
            126, -78, 83, 64, 41, 92, -113, -62, -11, 120, 86, 64, 1, 1, 1, 4,
            -21, 7, 1, 0, -128, -10, 40, 92, -113, -62, 53, 53, 64, 43, -121, 22,
            -39, -50, 55, 36, 64, -21, 7, 1, 0, -128, -14, -46, 77, 98, 16, -8,
            85, 64, 41, 92, -113, -62, -11, 48, 83, 64, -21, 7, 1, 0, -128, 96,
            -27, -48, 34, -37, 105, 80, 64, 47, -35, 36, 6, -127, 69, 75, 64, -21,
            7, 1, 0, -128, -10, 40, 92, -113, -62, 53, 53, 64, 43, -121, 22, -39,
            -50, 55, 36, 64, 1, 1, 1, 4, -21, 7, 1, 0, -128, -27, -48, 34,
            -37, -7, 62, 40, 64, -106, 67, -117, 108, -25, 59, 55, 64, -21, 7, 1,
            0, -128, 92, -113, -62, -11, 40, 44, 65, 64, -18, 124, 63, 53, 94, -70,
            70, 64, -21, 7, 1, 0, -128, 127, 106, -68, 116, -109, 72, 76, 64, 8,
            -84, 28, 90, 100, -21, 80, 64, -21, 7, 1, 0, -128, -27, -48, 34, -37,
            -7, 62, 40, 64, -106, 67, -117, 108, -25, 59, 55, 64, 69, 78, -60
        };
    }

    @Override
    protected KryoVertexiumSerializer getVertexiumSerializer() {
        return vertexiumSerializer;
    }

    @Test
    public void testObjectToValue() {
        byte[] valBytes = new KryoVertexiumSerializer().objectToBytes(new Date());
        assertEquals(10, valBytes.length);
    }

    @Test
    public void testLongString() {
        System.out.println("testLongString");
        timeItLongString(new KryoVertexiumSerializer());
        long quickKryoTime = timeItLongString(new QuickKryoVertexiumSerializer(graphConfiguration));
        long kryoTime = timeItLongString(new KryoVertexiumSerializer());
        assertTiming(quickKryoTime, kryoTime);
    }

    private long timeItLongString(VertexiumSerializer serializer) {
        return testItString(
            serializer,
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed consequat libero non nunc interdum " +
                "rutrum. Donec ac scelerisque libero, ut elementum ipsum. Donec placerat interdum nunc. Sed " +
                "et placerat mi. Morbi tristique, diam a cursus egestas, quam nisi sollicitudin justo, at " +
                "vehicula erat felis in risus. Nullam aliquet dictum eros eu cursus. Mauris id tellus cursus," +
                " malesuada purus sed, maximus mauris. Suspendisse non consectetur purus. Etiam faucibus ligula " +
                "eget ipsum porta finibus. Vestibulum vitae ornare ligula, eget molestie ligula. Praesent " +
                "consequat pulvinar sem vel tincidunt. Integer nisl est, gravida id luctus vel, hendrerit " +
                "non felis. Quisque sed risus consequat, tempor tellus sit amet, bibendum lorem."
        );
    }

    @Test
    public void testString() {
        System.out.println("testString");
        timeItString(new KryoVertexiumSerializer());
        long quickKryoTime = timeItString(new QuickKryoVertexiumSerializer(graphConfiguration));
        long kryoTime = timeItString(new KryoVertexiumSerializer());
        assertTiming(quickKryoTime, kryoTime);
    }

    private long timeItString(VertexiumSerializer serializer) {
        String string = "yo mamma";
        return testItString(serializer, string);
    }

    private long testItString(VertexiumSerializer serializer, String string) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < iterations; i++) {
            byte[] v = serializer.objectToBytes(string);
            assertEquals(string, serializer.bytesToObject(v));
            bytes += v.length;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(serializer.getClass().getName() + " time: " + (endTime - startTime) + "ms (size: " + new DecimalFormat("#,##0").format(bytes) + ")");
        return endTime - startTime;
    }

    @Test
    public void testBigDecimal() {
        System.out.println("testBigDecimal");
        timeItBigDecimal(new KryoVertexiumSerializer());
        long quickKryoTime = timeItBigDecimal(new QuickKryoVertexiumSerializer(graphConfiguration));
        long kryoTime = timeItBigDecimal(new KryoVertexiumSerializer());
        assertTiming(quickKryoTime, kryoTime);
    }

    private long timeItBigDecimal(VertexiumSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < iterations; i++) {
            byte[] v = serializer.objectToBytes(new BigDecimal("42.987654321"));
            assertEquals(new BigDecimal("42.987654321"), serializer.bytesToObject(v));
            bytes += v.length;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(serializer.getClass().getName() + " time: " + (endTime - startTime) + "ms (size: " + new DecimalFormat("#,##0").format(bytes) + ")");
        return endTime - startTime;
    }

    @Test
    public void testDate() {
        System.out.println("testDate");
        timeItDate(new KryoVertexiumSerializer());
        long quickKryoTime = timeItDate(new QuickKryoVertexiumSerializer(graphConfiguration));
        long kryoTime = timeItDate(new KryoVertexiumSerializer());
        assertTiming(quickKryoTime, kryoTime);
    }

    private long timeItDate(VertexiumSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < iterations; i++) {
            Date date = new Date();
            byte[] v = serializer.objectToBytes(date);
            assertEquals(date, serializer.bytesToObject(v));
            bytes += v.length;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(serializer.getClass().getName() + " time: " + (endTime - startTime) + "ms (size: " + new DecimalFormat("#,##0").format(bytes) + ")");
        return endTime - startTime;
    }

    @Test
    public void testLong() {
        System.out.println("testLong");
        timeItLong(new KryoVertexiumSerializer());
        long quickKryoTime = timeItLong(new QuickKryoVertexiumSerializer(graphConfiguration));
        long kryoTime = timeItLong(new KryoVertexiumSerializer());
        assertTiming(quickKryoTime, kryoTime);
    }

    private long timeItLong(VertexiumSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < iterations; i++) {
            Long l = 123456L;
            byte[] v = serializer.objectToBytes(l);
            assertEquals(l, serializer.bytesToObject(v));
            bytes += v.length;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(serializer.getClass().getName() + " time: " + (endTime - startTime) + "ms (size: " + new DecimalFormat("#,##0").format(bytes) + ")");
        return endTime - startTime;
    }

    @Test
    public void testDouble() {
        System.out.println("testDouble");
        timeItDouble(new KryoVertexiumSerializer());
        long quickKryoTime = timeItDouble(new QuickKryoVertexiumSerializer(graphConfiguration));
        long kryoTime = timeItDouble(new KryoVertexiumSerializer());
        assertTiming(quickKryoTime, kryoTime);
    }

    private long timeItDouble(VertexiumSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < iterations; i++) {
            double d = 3.1415;
            byte[] v = serializer.objectToBytes(d);
            assertEquals(d, serializer.bytesToObject(v), 0.001);
            bytes += v.length;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(serializer.getClass().getName() + " time: " + (endTime - startTime) + "ms (size: " + new DecimalFormat("#,##0").format(bytes) + ")");
        return endTime - startTime;
    }

    @Test
    public void testTestClass() {
        TestClass testClass = new TestClass("value1", 42);
        KryoVertexiumSerializer serializer = new KryoVertexiumSerializer();
        byte[] v = serializer.objectToBytes(testClass);
        assertEquals(testClass, serializer.bytesToObject(v));
    }

    @Test
    public void testTestClassQuick() {
        TestClass testClass = new TestClass("value1", 42);
        QuickKryoVertexiumSerializer serializer = new QuickKryoVertexiumSerializer(graphConfiguration);
        byte[] v = serializer.objectToBytes(testClass);
        assertEquals(testClass, serializer.bytesToObject(v));
    }

    private void assertTiming(long quickKryoTime, long kryoTime) {
        if (shouldAssertTiming) {
            // we can't fail when the timing is off because sometimes it's the JVMs fault doing garbage collection or something
            if (quickKryoTime > kryoTime) {
                System.err.println("WARNING: quick (" + quickKryoTime + "ms) was slower than Kryo (" + kryoTime + "ms)");
            }
        }
    }

    public static class TestClass {
        private String field1;
        private long field2;

        public TestClass(String field1, int field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestClass testClass = (TestClass) o;

            if (field2 != testClass.field2) return false;
            if (field1 != null ? !field1.equals(testClass.field1) : testClass.field1 != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = field1 != null ? field1.hashCode() : 0;
            result = 31 * result + (int) (field2 ^ (field2 >>> 32));
            return result;
        }
    }
}
