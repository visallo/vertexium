package org.vertexium.elasticsearch5;

import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.utils.SaveReadInputStream;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class LocalDiskStreamingPropertyValueService implements StreamingPropertyValueService {
    private final File directory;

    public LocalDiskStreamingPropertyValueService() {
        directory = new File("/tmp/localDiskStreamingPropertyValueService");
        directory.mkdirs();
    }

    @Override
    public org.vertexium.elasticsearch5.models.StreamingPropertyValueRef save(StreamingPropertyValue value) {
        String md5;
        long length = 0;
        try {
            MessageDigest md5Digest = MessageDigest.getInstance("MD5");
            File tempFile = File.createTempFile("temp", "temp", directory);
            InputStream in = value.getInputStream();
            if (String.class.equals(value.getValueType())) {
                in = new SaveReadInputStream(in, value.getLength());
            }
            try (FileOutputStream out = new FileOutputStream(tempFile)) {
                byte[] data = new byte[1000];
                int read;
                while ((read = in.read(data)) > 0) {
                    length += read;
                    md5Digest.update(data, 0, read);
                    out.write(data, 0, read);
                }
            } finally {
                in.close();
            }
            md5 = DatatypeConverter.printHexBinary(md5Digest.digest()).toLowerCase();
            File md5File = new File(directory, md5);
            if (md5File.exists()) {
                if (!tempFile.delete()) {
                    throw new VertexiumException("Could not delete temp file: " + tempFile.getAbsolutePath());
                }
            } else {
                if (!tempFile.renameTo(md5File)) {
                    throw new VertexiumException("Could not rename file " + tempFile.getAbsolutePath() + " to " + md5File.getAbsolutePath());
                }
            }

            org.vertexium.elasticsearch5.models.StreamingPropertyValueRef.Builder builder
                = org.vertexium.elasticsearch5.models.StreamingPropertyValueRef.newBuilder()
                .setValueType(value.getValueType().getName())
                .setLength(length)
                .setMd5(md5)
                .setSearchIndex(value.isSearchIndex());
            if (in instanceof SaveReadInputStream) {
                builder.setStringValue(((SaveReadInputStream) in).getString());
            }
            return builder
                .build();
        } catch (IOException | NoSuchAlgorithmException ex) {
            throw new VertexiumException("Could not save streaming property value", ex);
        }
    }

    @Override
    public StreamingPropertyValue read(StreamingPropertyValueRef<Elasticsearch5Graph> spvRef, long timestamp) {
        if (!(spvRef instanceof LocalDiskStreamingPropertyValueRef)) {
            throw new VertexiumException(String.format(
                "Invalid streaming property value ref. Expected %s found %s",
                LocalDiskStreamingPropertyValueRef.class.getName(),
                spvRef.getClass().getName()
            ));
        }

        LocalDiskStreamingPropertyValueRef localSpvRef = (LocalDiskStreamingPropertyValueRef) spvRef;
        File file = new File(directory, localSpvRef.getMd5());
        if (!file.exists()) {
            throw new VertexiumException("Could not find streaming property value file: " + file.getAbsolutePath());
        }
        return new StreamingPropertyValue(localSpvRef.getValueType()) {
            @Override
            public Long getLength() {
                return localSpvRef.getLength();
            }

            @Override
            public InputStream getInputStream() {
                try {
                    return new BufferedInputStream(new FileInputStream(file));
                } catch (FileNotFoundException ex) {
                    throw new VertexiumException("Could not read streaming property value: " + file.getAbsolutePath(), ex);
                }
            }
        };
    }

    @Override
    public StreamingPropertyValue fromProtobuf(org.vertexium.elasticsearch5.models.StreamingPropertyValueRef storedValue) {
        LocalDiskStreamingPropertyValueRef ref = new LocalDiskStreamingPropertyValueRef(
            storedValue.getValueType(),
            storedValue.getMd5(),
            storedValue.getLength(),
            storedValue.getSearchIndex()
        );
        return read(ref, 0);
    }
}
