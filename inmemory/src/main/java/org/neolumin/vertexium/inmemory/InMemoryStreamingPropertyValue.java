package org.neolumin.vertexium.inmemory;

import org.neolumin.vertexium.property.StreamingPropertyValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;

public class InMemoryStreamingPropertyValue extends StreamingPropertyValue implements Serializable {
    static final long serialVersionUID = 42L;
    private byte[] data;

    public InMemoryStreamingPropertyValue() {
        this(null, null);
    }

    public InMemoryStreamingPropertyValue(byte[] data, Class valueType) {
        super(null, valueType, data.length);
        this.data = data;
    }

    @Override
    public InputStream getInputStream() {
        return new ByteArrayInputStream(this.data);
    }
}
