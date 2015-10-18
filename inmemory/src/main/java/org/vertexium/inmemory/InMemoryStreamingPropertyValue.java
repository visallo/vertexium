package org.vertexium.inmemory;

import org.vertexium.property.StreamingPropertyValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class InMemoryStreamingPropertyValue extends StreamingPropertyValue {
    private byte[] data;

    public InMemoryStreamingPropertyValue(byte[] data, Class valueType) {
        super(null, valueType, data.length);
        this.data = data;
    }

    @Override
    public InputStream getInputStream() {
        return new ByteArrayInputStream(this.data);
    }
}
