package org.vertexium.accumulo;

import org.apache.hadoop.fs.Path;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

public class StreamingPropertyValueHdfsRef extends StreamingPropertyValueRef<AccumuloGraph> {
    private String path;

    // here for serialization
    protected StreamingPropertyValueHdfsRef() {

    }

    public StreamingPropertyValueHdfsRef(String path, StreamingPropertyValue propertyValue) {
        super(propertyValue);
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph, long timestamp) {
        return new StreamingPropertyValueHdfs(graph.getFileSystem(), new Path(graph.getDataDir(), getPath()), this);
    }
}
