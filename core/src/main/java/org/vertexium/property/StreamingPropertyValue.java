package org.vertexium.property;

import org.vertexium.VertexiumException;
import org.vertexium.util.StreamUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public class StreamingPropertyValue extends PropertyValue implements Serializable {
    static final long serialVersionUID = 42L;
    private final transient InputStream inputStream;
    private final Class valueType;
    private final long length;

    public StreamingPropertyValue() {
        this(null, null);
    }

    public StreamingPropertyValue(InputStream inputStream, Class valueType) {
        this(inputStream, valueType, -1);
    }

    public StreamingPropertyValue(InputStream inputStream, Class valueType, long length) {
        this.inputStream = inputStream;
        this.valueType = valueType;
        this.length = length;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public Class getValueType() {
        return valueType;
    }

    public long getLength() {
        return length;
    }

    public String readToString() {
        try {
            return StreamUtils.toString(getInputStream());
        } catch (IOException e) {
            throw new VertexiumException("Could not read streaming property value into string", e);
        }
    }

    public static StreamingPropertyValue create(String value) {
        InputStream data = new ByteArrayInputStream(value.getBytes());
        return new StreamingPropertyValue(data, String.class);
    }

    @Override
    public String toString() {
        return "StreamingPropertyValue{" +
                "valueType=" + valueType +
                ", length=" + length +
                '}';
    }
}
