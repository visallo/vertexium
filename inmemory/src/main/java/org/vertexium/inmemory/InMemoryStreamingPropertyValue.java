package org.vertexium.inmemory;

import org.vertexium.VertexiumException;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.util.StreamUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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

    public static Object saveStreamingPropertyValue(Object propertyValue) {
        try {
            if (propertyValue instanceof StreamingPropertyValue) {
                StreamingPropertyValue value = (StreamingPropertyValue) propertyValue;
                byte[] valueData = StreamUtils.toBytes(value.getInputStream());
                return new InMemoryStreamingPropertyValue(valueData, value.getValueType());
            }
            return propertyValue;
        } catch (IOException ex) {
            throw new VertexiumException("Could not save streaming property value", ex);
        }
    }
}
