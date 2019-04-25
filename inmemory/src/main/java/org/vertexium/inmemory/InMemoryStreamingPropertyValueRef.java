package org.vertexium.inmemory;

import org.vertexium.VertexiumException;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.util.IOUtils;

import java.io.IOException;

class InMemoryStreamingPropertyValueRef extends StreamingPropertyValueRef<InMemoryGraph> {
    private final byte[] valueData;

    InMemoryStreamingPropertyValueRef(StreamingPropertyValue value) {
        super(value);
        try {
            this.valueData = IOUtils.toBytes(value.getInputStream());
        } catch (IOException ex) {
            throw new VertexiumException("Could not read streaming property value", ex);
        }
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(InMemoryGraph graph, long timestamp) {
        return new InMemoryStreamingPropertyValue(
            valueData, getValueType()).searchIndex(isSearchIndex());

    }
}
