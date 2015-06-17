package org.vertexium.accumulo.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.EdgeInfo;
import org.vertexium.accumulo.StreamingPropertyValueHdfsRef;
import org.vertexium.accumulo.StreamingPropertyValueRef;
import org.vertexium.accumulo.StreamingPropertyValueTableRef;
import org.vertexium.accumulo.serializer.ValueSerializer;
import org.vertexium.type.GeoCircle;
import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoRect;

import java.util.Date;
import java.util.HashMap;

public class KryoValueSerializer implements ValueSerializer {
    private final Kryo kryo;

    public KryoValueSerializer() {
        kryo = new Kryo(new DefaultClassResolver(), new MapReferenceResolver() {
            @Override
            public boolean useReferences(Class type) {
                if (type == String.class || type == Date.class) {
                    return false;
                }
                return super.useReferences(type);
            }
        });
        kryo.register(EdgeInfo.class, 1000);
        kryo.register(GeoPoint.class, 1001);
        kryo.register(HashMap.class, 1002);
        kryo.register(StreamingPropertyValueRef.class, 1003);
        kryo.register(StreamingPropertyValueTableRef.class, 1004);
        kryo.register(StreamingPropertyValueHdfsRef.class, 1005);
        kryo.register(GeoRect.class, 1006);
        kryo.register(GeoCircle.class, 1007);
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
