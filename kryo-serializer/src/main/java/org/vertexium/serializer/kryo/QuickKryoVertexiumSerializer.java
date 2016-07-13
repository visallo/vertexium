package org.vertexium.serializer.kryo;

import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;
import org.vertexium.VertexiumSerializer;
import org.vertexium.serializer.kryo.quickSerializers.*;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class QuickKryoVertexiumSerializer implements VertexiumSerializer {
    private static final byte[] EMPTY = new byte[0];
    public static final String CONFIG_COMPRESS = GraphConfiguration.SERIALIZER + ".enableCompression";
    public static final boolean CONFIG_COMPRESS_DEFAULT = false;
    private final boolean enableCompression;
    private QuickTypeSerializer defaultQuickTypeSerializer = new KryoQuickTypeSerializer();
    private Map<Class, QuickTypeSerializer> quickTypeSerializersByClass = new HashMap<Class, QuickTypeSerializer>() {{
        put(String.class, new StringQuickTypeSerializer());
        put(Long.class, new LongQuickTypeSerializer());
        put(Date.class, new DateQuickTypeSerializer());
        put(Double.class, new DoubleQuickTypeSerializer());
        put(BigDecimal.class, new BigDecimalQuickTypeSerializer());
    }};
    private Map<Byte, QuickTypeSerializer> quickTypeSerializersByMarker = new HashMap<Byte, QuickTypeSerializer>() {{
        put(QuickTypeSerializer.MARKER_KRYO, new KryoQuickTypeSerializer());
        put(QuickTypeSerializer.MARKER_STRING, new StringQuickTypeSerializer());
        put(QuickTypeSerializer.MARKER_LONG, new LongQuickTypeSerializer());
        put(QuickTypeSerializer.MARKER_DATE, new DateQuickTypeSerializer());
        put(QuickTypeSerializer.MARKER_DOUBLE, new DoubleQuickTypeSerializer());
        put(QuickTypeSerializer.MARKER_BIG_DECIMAL, new BigDecimalQuickTypeSerializer());
    }};

    public QuickKryoVertexiumSerializer(GraphConfiguration config) {
        enableCompression = config.getBoolean(CONFIG_COMPRESS, CONFIG_COMPRESS_DEFAULT);
    }

    @Override
    public byte[] objectToBytes(Object object) {
        if (object == null) {
            return EMPTY;
        }
        QuickTypeSerializer quickTypeSerializer = quickTypeSerializersByClass.get(object.getClass());
        byte[] bytes;
        if (quickTypeSerializer != null) {
            bytes = quickTypeSerializer.objectToBytes(object);
        } else {
            bytes = defaultQuickTypeSerializer.objectToBytes(object);
        }
        return compress(bytes);
    }

    @Override
    public <T> T bytesToObject(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        bytes = expand(bytes);
        QuickTypeSerializer quickTypeSerializer = quickTypeSerializersByMarker.get(bytes[0]);
        if (quickTypeSerializer != null) {
            return quickTypeSerializer.valueToObject(bytes);
        }
        throw new VertexiumException("Invalid marker: " + Integer.toHexString(bytes[0]));
    }

    protected byte[] compress(byte[] bytes) {
        if (!enableCompression) {
            return bytes;
        }

        try {
            Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
            deflater.setInput(bytes);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(bytes.length);
            deflater.finish();
            byte[] buffer = new byte[1024];
            while (!deflater.finished()) {
                int count = deflater.deflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            outputStream.close();
            return outputStream.toByteArray();
        } catch (Exception ex) {
            throw new VertexiumException("Could not compress bytes", ex);
        }
    }

    protected byte[] expand(byte[] bytes) {
        if (!enableCompression) {
            return bytes;
        }

        try {
            Inflater inflater = new Inflater();
            inflater.setInput(bytes);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(bytes.length);
            byte[] buffer = new byte[1024];
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            outputStream.close();

            return outputStream.toByteArray();
        } catch (Exception ex) {
            throw new VertexiumException("Could not decompress bytes", ex);
        }
    }
}
