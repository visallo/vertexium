package org.vertexium.accumulo.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import org.apache.accumulo.core.data.Value;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
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
    private final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo(new DefaultClassResolver(), new MapReferenceResolver() {
                @Override
                public boolean useReferences(Class type) {
                    // avoid calling System.identityHashCode
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
            kryo.register(Date.class, 1008);
            kryo.setAutoReset(true);
            return kryo;
        }
    };

    @Override
    public Value objectToValue(Object value) {
        Output output = new UnsafeOutput(2000, -1);
        kryo.get().writeClassAndObject(output, value);
        return new Value(output.toBytes());
    }

    @Override
    public <T> T valueToObject(Value value) {
        return valueToObject(value.get());
    }

    @Override
    public <T> T valueToObject(byte[] data) {
        Input input = new UnsafeInput(data);
        return (T) kryo.get().readClassAndObject(input);
    }
}
