package org.vertexium;

import org.vertexium.property.StreamingPropertyValue;

/**
 * A chunk of a {@link StreamingPropertyValue}
 */
public class StreamingPropertyValueChunk {
    private final StreamingPropertyValue streamingPropertyValue;
    private final byte[] data;
    private final int chunkSize;
    private final boolean last;

    public StreamingPropertyValueChunk(StreamingPropertyValue streamingPropertyValue, byte[] data, int chunkSize, boolean last) {
        this.streamingPropertyValue = streamingPropertyValue;
        this.data = data;
        this.chunkSize = chunkSize;
        this.last = last;
    }

    public StreamingPropertyValue getStreamingPropertyValue() {
        return streamingPropertyValue;
    }

    public byte[] getData() {
        return data;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public boolean isLast() {
        return last;
    }
}
