package org.neolumin.vertexium.inmemory;

import org.neolumin.vertexium.property.StreamingPropertyValue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

class InMemoryStreamingPropertyValue extends StreamingPropertyValue {
    private byte[] data;

    InMemoryStreamingPropertyValue(byte[] data, Class valueType) throws IOException {
        super(null, valueType, data.length);
        this.data = data;
    }

    @Override
    public InputStream getInputStream() {
        return new ByteArrayInputStream(this.data);
    }
}
