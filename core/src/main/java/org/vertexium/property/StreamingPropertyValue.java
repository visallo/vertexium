package org.vertexium.property;

import org.vertexium.VertexiumException;
import org.vertexium.util.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public abstract class StreamingPropertyValue extends PropertyValue implements Serializable {
    private static final long serialVersionUID = -8009009221695795406L;
    private final Class valueType;

    public StreamingPropertyValue(Class valueType) {
        this.valueType = valueType;
    }

    public Class getValueType() {
        return valueType;
    }

    public abstract Long getLength();

    public abstract InputStream getInputStream();

    public String readToString() {
        try (InputStream in = getInputStream()) {
            return IOUtils.toString(in);
        } catch (IOException e) {
            throw new VertexiumException("Could not read streaming property value into string", e);
        }
    }

    public String readToString(long offset, long limit) {
        try (InputStream in = getInputStream()) {
            return IOUtils.toString(in, offset, limit);
        } catch (IOException e) {
            throw new VertexiumException("Could not read streaming property value into string", e);
        }
    }

    public static StreamingPropertyValue create(String value) {
        InputStream data = new ByteArrayInputStream(value.getBytes());
        return new DefaultStreamingPropertyValue(data, String.class);
    }

    public static StreamingPropertyValue create(InputStream inputStream, Class type, Long length) {
        return new DefaultStreamingPropertyValue(inputStream, type, length);
    }

    public static StreamingPropertyValue create(InputStream inputStream, Class type) {
        return new DefaultStreamingPropertyValue(inputStream, type, null);
    }

    @Override
    public String toString() {
        return "StreamingPropertyValue{" +
                "valueType=" + getValueType() +
                ", length=" + getLength() +
                '}';
    }
}
