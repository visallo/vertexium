package org.vertexium;

import org.vertexium.property.StreamingPropertyValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class StreamingPropertyValueData {
    private final StreamingPropertyValue streamingPropertyValue;
    private final byte[] data;
    private final int size;

    public StreamingPropertyValueData(StreamingPropertyValue streamingPropertyValue, byte[] data, int size) {
        this.streamingPropertyValue = streamingPropertyValue;
        this.data = data;
        this.size = size;
    }

    /**
     * This property is here to get a reference to the {@link StreamingPropertyValue} that this data came from and
     * should not be read from using methods such as {@link StreamingPropertyValue#getInputStream()} or
     * {@link StreamingPropertyValue#readToString()}.
     */
    public StreamingPropertyValue getStreamingPropertyValue() {
        return streamingPropertyValue;
    }

    public byte[] getData() {
        return data;
    }

    public String getDataAsString() {
        return new String(getData(), StandardCharsets.UTF_8);
    }

    public int getSize() {
        return size;
    }

    public InputStream getInputStream() {
        return new ByteArrayInputStream(getData(), 0, getSize());
    }
}
