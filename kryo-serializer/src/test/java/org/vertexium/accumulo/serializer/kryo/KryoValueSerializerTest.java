package org.vertexium.accumulo.serializer.kryo;

import org.apache.accumulo.core.data.Value;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;

public class KryoValueSerializerTest {
    @Test
    public void testObjectToValue() {
        Value val = new KryoValueSerializer().objectToValue(new Date());
        byte[] valBytes = val.get();
        assertEquals(10, valBytes.length);
    }
}