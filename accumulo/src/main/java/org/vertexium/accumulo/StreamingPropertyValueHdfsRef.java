package org.vertexium.accumulo;

import org.apache.hadoop.fs.Path;
import org.vertexium.accumulo.util.OverflowIntoHdfsStreamingPropertyValueStorageStrategy;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

public class StreamingPropertyValueHdfsRef extends StreamingPropertyValueRef<AccumuloGraph> {
    private static final long serialVersionUID = -7075231119033637091L;
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
        OverflowIntoHdfsStreamingPropertyValueStorageStrategy writer = (OverflowIntoHdfsStreamingPropertyValueStorageStrategy) graph.getStreamingPropertyValueStorageStrategy();
        return new StreamingPropertyValueHdfs(writer.getFileSystem(), new Path(writer.getDataDir(), getPath()), this);
    }
}
