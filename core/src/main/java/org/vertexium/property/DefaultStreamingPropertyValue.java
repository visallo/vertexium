package org.vertexium.property;

import java.io.InputStream;

public class DefaultStreamingPropertyValue extends StreamingPropertyValue {
    private static final long serialVersionUID = 6520945094293028859L;
    private final transient InputStream inputStream;
    private final Long length;

    public DefaultStreamingPropertyValue() {
        this(null, null);
    }

    public DefaultStreamingPropertyValue(InputStream inputStream, Class valueType) {
        this(inputStream, valueType, null);
    }

    public DefaultStreamingPropertyValue(InputStream inputStream, Class valueType, Long length) {
        super(valueType);
        this.inputStream = inputStream;
        this.length = length;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public Long getLength() {
        return length;
    }
}
