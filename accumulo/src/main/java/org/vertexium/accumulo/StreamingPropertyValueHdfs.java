package org.vertexium.accumulo;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.vertexium.VertexiumException;
import org.vertexium.property.StreamingPropertyValue;

import java.io.IOException;
import java.io.InputStream;

class StreamingPropertyValueHdfs extends StreamingPropertyValue {
    private static final long serialVersionUID = 5936794077542255789L;
    private final FileSystem fs;
    private final Path path;

    public StreamingPropertyValueHdfs(FileSystem fs, Path path, StreamingPropertyValueHdfsRef streamingPropertyValueRef) {
        super(streamingPropertyValueRef.getValueType());
        this.searchIndex(streamingPropertyValueRef.isSearchIndex());
        this.fs = fs;
        this.path = path;
    }

    @Override
    public Long getLength() {
        try {
            return fs.getFileStatus(path).getLen();
        } catch (IOException ex) {
            throw new VertexiumException("Could not get length of: " + this.path, ex);
        }
    }

    @Override
    public InputStream getInputStream() {
        try {
            return fs.open(this.path);
        } catch (IOException ex) {
            throw new VertexiumException("Could not open: " + this.path, ex);
        }
    }
}
