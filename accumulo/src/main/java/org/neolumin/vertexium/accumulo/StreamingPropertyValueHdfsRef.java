package org.neolumin.vertexium.accumulo;

import org.apache.hadoop.fs.Path;
import org.neolumin.vertexium.property.StreamingPropertyValue;

public class StreamingPropertyValueHdfsRef extends StreamingPropertyValueRef {
    private String path;

    protected StreamingPropertyValueHdfsRef() {
        super();
        this.path = null;
    }

    public StreamingPropertyValueHdfsRef(String path, StreamingPropertyValue propertyValue) {
        super(propertyValue);
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph) {
        return new StreamingPropertyValueHdfs(graph.getFileSystem(), new Path(graph.getDataDir(), getPath()), this);
    }
}
