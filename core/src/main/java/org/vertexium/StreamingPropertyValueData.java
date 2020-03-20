package org.vertexium;

import org.vertexium.property.StreamingPropertyValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class StreamingPropertyValueData {
    private final StreamingPropertyValue streamingPropertyValue;
    private final byte[] data;
    private final int size;

    public StreamingPropertyValueData(StreamingPropertyValue streamingPropertyValue, byte[] data, int size) {
        this.streamingPropertyValue = streamingPropertyValue;
        this.data = data;
        this.size = size;
    }

    public StreamingPropertyValue getStreamingPropertyValue() {
        return streamingPropertyValue;
    }

    public byte[] getData() {
        return data;
    }

    public int getSize() {
        return size;
    }

    public InputStream getInputStream() {
        return new ByteArrayInputStream(getData(), 0, getSize());
    }
}
