package org.vertexium.accumulo.serializer.kryo;

import org.apache.accumulo.core.data.Value;
import org.junit.Test;
import org.vertexium.accumulo.serializer.ValueSerializer;

import java.text.DecimalFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KryoValueSerializerTest {
    @Test
    public void testObjectToValue() {
        Value val = new KryoValueSerializer().objectToValue(new Date());
        byte[] valBytes = val.get();
        assertEquals(10, valBytes.length);
    }

    @Test
    public void testString() {
        System.out.println("testString");
        timeItString(new KryoValueSerializer());
        long quickKryoTime = timeItString(new QuickKryoValueSerializer());
        long kryoTime = timeItString(new KryoValueSerializer());
        assertTrue("quick was slower than Kryo", quickKryoTime < kryoTime);
    }

    private long timeItString(ValueSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < 1000000; i++) {
            Value v = serializer.objectToValue("yo mamma");
            assertEquals("yo mamma", serializer.valueToObject(v.get()));
            bytes += v.get().length;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(serializer.getClass().getName() + " time: " + (endTime - startTime) + "ms (size: " + new DecimalFormat("#,##0").format(bytes) + ")");
        return endTime - startTime;
    }

    @Test
    public void testDate() {
        System.out.println("testDate");
        timeItDate(new KryoValueSerializer());
        long quickKryoTime = timeItDate(new QuickKryoValueSerializer());
        long kryoTime = timeItDate(new KryoValueSerializer());
        assertTrue("quick was slower than Kryo", quickKryoTime < kryoTime);
    }

    private long timeItDate(ValueSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < 1000000; i++) {
            Date date = new Date();
            Value v = serializer.objectToValue(date);
            assertEquals(date, serializer.valueToObject(v.get()));
            bytes += v.get().length;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(serializer.getClass().getName() + " time: " + (endTime - startTime) + "ms (size: " + new DecimalFormat("#,##0").format(bytes) + ")");
        return endTime - startTime;
    }

    @Test
    public void testLong() {
        System.out.println("testLong");
        timeItLong(new KryoValueSerializer());
        long quickKryoTime = timeItLong(new QuickKryoValueSerializer());
        long kryoTime = timeItLong(new KryoValueSerializer());
        assertTrue("quick was slower than Kryo", quickKryoTime < kryoTime);
    }

    private long timeItLong(ValueSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < 1000000; i++) {
            Long l = 123456L;
            Value v = serializer.objectToValue(l);
            assertEquals(l, serializer.valueToObject(v.get()));
            bytes += v.get().length;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(serializer.getClass().getName() + " time: " + (endTime - startTime) + "ms (size: " + new DecimalFormat("#,##0").format(bytes) + ")");
        return endTime - startTime;
    }

    @Test
    public void testDouble() {
        System.out.println("testDouble");
        timeItDouble(new KryoValueSerializer());
        long quickKryoTime = timeItDouble(new QuickKryoValueSerializer());
        long kryoTime = timeItDouble(new KryoValueSerializer());
        assertTrue("quick was slower than Kryo", quickKryoTime < kryoTime);
    }

    private long timeItDouble(ValueSerializer serializer) {
        long startTime = System.currentTimeMillis();
        long bytes = 0;
        for (int i = 0; i < 1000000; i++) {
            double d = 3.1415;
            Value v = serializer.objectToValue(d);
            assertEquals(d, serializer.valueToObject(v.get()));
            bytes += v.get().length;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(serializer.getClass().getName() + " time: " + (endTime - startTime) + "ms (size: " + new DecimalFormat("#,##0").format(bytes) + ")");
        return endTime - startTime;
    }

    @Test
    public void testTestClass() {
        TestClass testClass = new TestClass("value1", 42);
        QuickKryoValueSerializer serializer = new QuickKryoValueSerializer();
        Value v = serializer.objectToValue(testClass);
        assertEquals(testClass, serializer.valueToObject(v.get()));
    }

    public static class TestClass {
        private String field1;
        private long field2;

        public TestClass() {
            
        }

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