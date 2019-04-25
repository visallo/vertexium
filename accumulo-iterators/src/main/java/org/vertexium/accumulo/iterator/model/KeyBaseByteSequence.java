package org.vertexium.accumulo.iterator.model;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.util.ByteSequenceUtils;

import java.nio.ByteBuffer;

import static org.vertexium.accumulo.iterator.util.ByteSequenceUtils.indexOf;
import static org.vertexium.accumulo.iterator.util.ByteSequenceUtils.subSequence;

public abstract class KeyBaseByteSequence {
    public static final byte VALUE_SEPARATOR = 0x1f;

    public static ByteSequence[] splitOnValueSeparator(Text text, int partCount) {
        return splitOnValueSeparator(new ArrayByteSequence(text.getBytes()), partCount);
    }

    public static ByteSequence[] splitOnValueSeparator(ByteSequence bytes, int partCount) {
        ByteSequence[] results = new ByteSequence[partCount];
        int last = 0;
        int i = indexOf(bytes, VALUE_SEPARATOR);
        int partIndex = 0;
        while (true) {
            if (i > 0) {
                results[partIndex++] = subSequence(bytes, last, i);
                if (partIndex >= partCount) {
                    throw new VertexiumAccumuloIteratorException("Invalid number of parts for '" + bytes + "'. Expected " + partCount + " found " + partIndex);
                }
                last = i + 1;
                i = indexOf(bytes, VALUE_SEPARATOR, last);
            } else {
                results[partIndex++] = subSequence(bytes, last);
                break;
            }
        }
        if (partIndex != partCount) {
            throw new VertexiumAccumuloIteratorException("Invalid number of parts for '" + bytes + "'. Expected " + partCount + " found " + partIndex);
        }
        return results;
    }

    public static void assertNoValueSeparator(ByteSequence bytes) {
        if (indexOf(bytes, VALUE_SEPARATOR) >= 0) {
            throw new VertexiumInvalidKeyException("String cannot contain '" + VALUE_SEPARATOR + "' (0x1f): " + bytes);
        }
    }

    public static ByteSequence getDiscriminator(ByteSequence propertyName, ByteSequence propertyKey, ByteSequence visibilityString, long timestamp) {
        assertNoValueSeparator(propertyName);
        assertNoValueSeparator(propertyKey);
        assertNoValueSeparator(visibilityString);
        int length = propertyName.length() + propertyKey.length() + visibilityString.length() + 8;
        byte[] bytes = new byte[length];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        ByteSequenceUtils.putIntoByteBuffer(propertyName, bb);
        ByteSequenceUtils.putIntoByteBuffer(propertyKey, bb);
        ByteSequenceUtils.putIntoByteBuffer(visibilityString, bb);
        bb.putLong(timestamp);
        return new ArrayByteSequence(bytes);
    }
}

