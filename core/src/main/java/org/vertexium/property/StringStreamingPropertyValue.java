package org.vertexium.property;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class StringStreamingPropertyValue extends StreamingPropertyValue {
    private static final long serialVersionUID = 8609536455398889081L;
    private final String value;

    public StringStreamingPropertyValue() {
        this(null);
    }

    public StringStreamingPropertyValue(String value) {
        super(String.class);
        this.value = value;
    }

    @Override
    public String readToString() {
        return value;
    }

    public InputStream getInputStream() {
        byte[] bytes = value == null ? new byte[0] : value.getBytes(UTF_8);
        return new ByteArrayInputStream(bytes);
    }

    public Long getLength() {
        return value == null ? 0 : Long.valueOf(value.getBytes(UTF_8).length);
    }
}
