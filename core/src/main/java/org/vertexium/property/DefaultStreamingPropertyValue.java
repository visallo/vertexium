package org.vertexium.property;

import org.vertexium.VertexiumException;
import org.vertexium.util.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DefaultStreamingPropertyValue extends StreamingPropertyValue {
    private static final long serialVersionUID = 6520945094293028859L;
    private final transient InputStream inputStream;
    private transient boolean inputStreamUsed;
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
        if (inputStream instanceof ByteArrayInputStream) {
            synchronized (inputStream) {
                ByteArrayInputStream bais = (ByteArrayInputStream) inputStream;
                bais.mark(bais.available());
                try {
                    byte[] data = IOUtils.toBytes(bais);
                    return new ByteArrayInputStream(data);
                } catch (IOException e) {
                    throw new VertexiumException("Could not read input stream", e);
                } finally {
                    bais.reset();
                }
            }
        }
        if (inputStreamUsed) {
            throw new VertexiumException("Input stream already consumed");
        }
        inputStreamUsed = true;
        return inputStream;
    }

    public Long getLength() {
        return length;
    }
}
