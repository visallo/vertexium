package org.vertexium.serializer.kryo;

import org.junit.Before;
import org.junit.Test;
import org.vertexium.VertexiumSerializer;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class KryoVertexiumSerializerTest {
    @Before
    public void before() {
        System.gc();
    }

    @Test
    public void testObjectToValue() {
        byte[] valBytes = new KryoVertexiumSerializer().objectToBytes(new Date());
        assertEquals(10, valBytes.length);
    }

    @Test
    public void testString() {
        System.out.println("testString");
        timeItString(new KryoVertexiumSerializer());
        long quickKryoTime = timeItString(new QuickKryoVertexiumSerializer());
        long kryoTime = timeItString(new KryoVertexiumSerializer());
        assertTiming(quickKryoTime, kryoTime);
    }

    private long timeItString(VertexiumSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < 1000000; i++) {
            byte[] v = serializer.objectToBytes("yo mamma");
            assertEquals("yo mamma", serializer.bytesToObject(v));
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
        long quickKryoTime = timeItBigDecimal(new QuickKryoVertexiumSerializer());
        long kryoTime = timeItBigDecimal(new KryoVertexiumSerializer());
        assertTiming(quickKryoTime, kryoTime);
    }

    private long timeItBigDecimal(VertexiumSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < 1000000; i++) {
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
        long quickKryoTime = timeItDate(new QuickKryoVertexiumSerializer());
        long kryoTime = timeItDate(new KryoVertexiumSerializer());
        assertTiming(quickKryoTime, kryoTime);
    }

    private long timeItDate(VertexiumSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < 1000000; i++) {
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
        long quickKryoTime = timeItLong(new QuickKryoVertexiumSerializer());
        long kryoTime = timeItLong(new KryoVertexiumSerializer());
        assertTiming(quickKryoTime, kryoTime);
    }

    private long timeItLong(VertexiumSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < 1000000; i++) {
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
        long quickKryoTime = timeItDouble(new QuickKryoVertexiumSerializer());
        long kryoTime = timeItDouble(new KryoVertexiumSerializer());
        assertTiming(quickKryoTime, kryoTime);
    }

    private long timeItDouble(VertexiumSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < 1000000; i++) {
            double d = 3.1415;
            byte[] v = serializer.objectToBytes(d);
            assertEquals(d, serializer.bytesToObject(v));
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
        QuickKryoVertexiumSerializer serializer = new QuickKryoVertexiumSerializer();
        byte[] v = serializer.objectToBytes(testClass);
        assertEquals(testClass, serializer.bytesToObject(v));
    }

    private void assertTiming(long quickKryoTime, long kryoTime) {
        // we can't fail when the timing is off because sometimes it's the JVMs fault doing garbage collection or something
        if (quickKryoTime > kryoTime) {
            System.err.println("WARNING: quick (" + quickKryoTime + "ms) was slower than Kryo (" + kryoTime + "ms)");
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
