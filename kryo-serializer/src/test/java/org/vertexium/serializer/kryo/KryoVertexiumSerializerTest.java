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
    protected byte[] getSerializableObjectBytes() {
        return new byte[]{
                1, 0, -61, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
                109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
                83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
                115, 101, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79,
                98, 106, 101, 99, 116, 115, 1, 69, 78, -60, 1, 1, 111, 114, 103, 46,
                118, 101, 114, 116, 101, 120, 105, 117, 109, 46, 112, 114, 111, 112, 101, 114,
                116, 121, 46, 80, 114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117, -27,
                1, 1, 1, 83, 84, 65, 82, -44, 1, 2, 111, 114, 103, 46, 118, 101,
                114, 116, 101, 120, 105, 117, 109, 46, 112, 114, 111, 112, 101, 114, 116, 121,
                46, 68, 101, 102, 97, 117, 108, 116, 83, 116, 114, 101, 97, 109, 105, 110,
                103, 80, 114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117, -27, 1, 1,
                4, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 3, 91, -62, 0,
                1, 4, -51, 1, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117,
                109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109,
                83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97,
                115, 101, 36, 84, 101, 115, 116, 83, 116, 114, 101, 97, 109, 105, 110, 103,
                80, 114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117, 101, 82, 101, 102,
                1, 1, 1, 91, -62
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
