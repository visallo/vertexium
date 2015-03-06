package org.neolumin.vertexium.accumulo.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.accumulo.core.data.Value;
import org.neolumin.vertexium.accumulo.EdgeInfo;
import org.neolumin.vertexium.accumulo.StreamingPropertyValueHdfsRef;
import org.neolumin.vertexium.accumulo.StreamingPropertyValueRef;
import org.neolumin.vertexium.accumulo.StreamingPropertyValueTableRef;
import org.neolumin.vertexium.accumulo.serializer.ValueSerializer;
import org.neolumin.vertexium.type.GeoPoint;

import java.util.HashMap;

public class KryoValueSerializer implements ValueSerializer {
    private final Kryo kryo;

    public KryoValueSerializer() {
        kryo = new Kryo();
        kryo.register(EdgeInfo.class, 1000);
        kryo.register(GeoPoint.class, 1001);
        kryo.register(HashMap.class, 1002);
        kryo.register(StreamingPropertyValueRef.class, 1003);
        kryo.register(StreamingPropertyValueTableRef.class, 1004);
        kryo.register(StreamingPropertyValueHdfsRef.class, 1005);
    }

    @Override
    public Value objectToValue(Object value) {
        Output output = new Output(2000);
        kryo.writeClassAndObject(output, value);
        return new Value(output.toBytes());
    }

    @Override
    public <T> T valueToObject(Value value) {
        return valueToObject(value.get());
    }

    @Override
    public <T> T valueToObject(byte[] data) {
        Input input = new Input(data);
        return (T) kryo.readClassAndObject(input);
    }
}
