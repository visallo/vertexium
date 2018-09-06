package org.vertexium.serializer.kryo;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.vertexium.test.VertexiumSerializerTestBase;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class QuickKryoVertexiumSerializerTest extends VertexiumSerializerTestBase {
    private final boolean compress;
    private QuickKryoVertexiumSerializer vertexiumSerializer;

    @Parameterized.Parameters(name = "compress={0}")
    public static Iterable<Object[]> initialVisibilitySources() {
        return Arrays.asList(new Object[][]{
                {true}, {false}
        });
    }

    public QuickKryoVertexiumSerializerTest(boolean compress) {
        this.compress = compress;
    }

    @Before
    public void before() {
        vertexiumSerializer = new QuickKryoVertexiumSerializer(compress);
    }

    @Test
    public void testCompress() {
        String testString = "This is a test value";
        QuickKryoVertexiumSerializer serializer = new QuickKryoVertexiumSerializer(true);
        byte[] bytes = serializer.objectToBytes(testString);
        Object str = serializer.bytesToObject(bytes);
        assertEquals(testString, str);
    }

    @Override
    protected byte[] getSerializableObjectBytes() {
        if (compress) {
            return new byte[]{
                    120, -38, 99, 96, 100, 56, -52, -104, 95, -108, -82, 87, -106, 90, 84, -110,
                    90, -111, 89, -102, -85, 87, -110, 90, 92, -94, 23, 6, -29, 6, -89, 22,
                    101, 38, -26, 100, 86, -91, 22, -123, 0, -59, -99, 18, -117, 83, 85, 96,
                    66, -119, 73, 57, -87, -2, 73, 89, -87, -55, 37, -59, -116, -82, 126, 71,
                    24, -47, -52, 41, 40, -54, 47, 0, 114, 42, -11, 2, -96, -116, -80, -60,
                    -100, -46, -89, -116, -116, -116, -63, 33, -114, 65, 87, 24, -103, 112, -88, 118,
                    73, 77, 75, 44, -51, 41, 9, 46, 41, 74, 77, -52, -51, -52, 75, 71,
                    -45, -51, -62, 0, 1, 64, 115, 24, -103, -93, 15, 49, 48, -78, -100, 37,
                    -43, -3, 32, 6, 86, -29, 83, -125, 82, -45, 24, 25, -93, 15, 1, 0,
                    -7, -93, 100, -89
            };
        } else {
            return new byte[]{
                    0, 1, 0, -61, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105,
                    117, 109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117,
                    109, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66,
                    97, 115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101,
                    79, 98, 106, 101, 99, 116, 115, 1, 69, 78, -60, 1, 1, 111, 114, 103,
                    46, 118, 101, 114, 116, 101, 120, 105, 117, 109, 46, 112, 114, 111, 112, 101,
                    114, 116, 121, 46, 80, 114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117,
                    -27, 1, 1, 1, 83, 84, 65, 82, -44, 1, 2, 111, 114, 103, 46, 118,
                    101, 114, 116, 101, 120, 105, 117, 109, 46, 112, 114, 111, 112, 101, 114, 116,
                    121, 46, 68, 101, 102, 97, 117, 108, 116, 83, 116, 114, 101, 97, 109, 105,
                    110, 103, 80, 114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117, -27, 1,
                    1, 4, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 3, 91, -62,
                    0, 1, 4, -51, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105,
                    117, 109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117,
                    109, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66,
                    97, 115, 101, 36, 84, 101, 115, 116, 83, 116, 114, 101, 97, 109, 105, 110,
                    103, 80, 114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117, 101, 82, 101,
                    102, 1, 1, 91, -62
            };
        }
    }

    @Override
    protected QuickKryoVertexiumSerializer getVertexiumSerializer() {
        return vertexiumSerializer;
    }
}