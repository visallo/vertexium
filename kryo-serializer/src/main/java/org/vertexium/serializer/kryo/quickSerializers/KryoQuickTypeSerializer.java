package org.vertexium.serializer.kryo.quickSerializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import org.vertexium.serializer.kryo.KryoFactory;

public class KryoQuickTypeSerializer implements QuickTypeSerializer {
    private final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            return new KryoFactory().createKryo();
        }
    };

    @Override
    public byte[] objectToBytes(Object value) {
        Output output = new UnsafeOutput(2000, -1);
        output.writeByte(MARKER_KRYO);
        kryo.get().writeClassAndObject(output, value);
        return output.toBytes();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T valueToObject(byte[] data) {
        Input input = new UnsafeInput(data);
        input.read();
        return (T) kryo.get().readClassAndObject(input);
    }
}
