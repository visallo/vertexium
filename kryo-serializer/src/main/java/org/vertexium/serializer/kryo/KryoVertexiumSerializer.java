package org.vertexium.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import org.vertexium.VertexiumSerializer;

public class KryoVertexiumSerializer implements VertexiumSerializer {
    private static final byte[] EMPTY = new byte[0];
    private final ThreadLocal<Kryo> kryo = ThreadLocal.withInitial(() -> {
        return new KryoFactory().createKryo();
    });

    @Override
    public byte[] objectToBytes(Object object) {
        if (object == null) {
            return EMPTY;
        }
        Output output = new UnsafeOutput(2000, -1);
        kryo.get().writeClassAndObject(output, object);
        return output.toBytes();
    }

    @Override
    public <T> T bytesToObject(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        Input input = new UnsafeInput(bytes);
        return (T) kryo.get().readClassAndObject(input);
    }
}
