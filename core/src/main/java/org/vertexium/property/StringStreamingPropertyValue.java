package org.vertexium.property;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class StringStreamingPropertyValue extends StreamingPropertyValue {
    private static final long serialVersionUID = -3205612435159547402L;
    private final String value;

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
        return value == null ? 0 : (long) value.getBytes(UTF_8).length;
    }
}
