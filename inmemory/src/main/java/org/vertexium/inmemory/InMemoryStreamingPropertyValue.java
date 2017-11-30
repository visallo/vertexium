package org.vertexium.inmemory;

import org.vertexium.property.DefaultStreamingPropertyValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class InMemoryStreamingPropertyValue extends DefaultStreamingPropertyValue {
    private static final long serialVersionUID = 8822712986450929357L;
    private byte[] data;

    public InMemoryStreamingPropertyValue(byte[] data, Class valueType) {
        super(null, valueType, (long) data.length);
        this.data = data;
    }

    @Override
    public InputStream getInputStream() {
        return new ByteArrayInputStream(this.data);
    }
}
