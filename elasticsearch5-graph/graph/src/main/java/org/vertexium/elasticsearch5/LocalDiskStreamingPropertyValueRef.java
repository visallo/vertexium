package org.vertexium.elasticsearch5;

import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

public class LocalDiskStreamingPropertyValueRef extends StreamingPropertyValueRef<Elasticsearch5Graph> {
    private static final long serialVersionUID = -5402040433177689945L;
    private final long length;
    private final String md5;

    public LocalDiskStreamingPropertyValueRef(StreamingPropertyValue spv, long length, String md5) {
        super(spv);
        this.length = length;
        this.md5 = md5;
    }

    public LocalDiskStreamingPropertyValueRef(
        String valueType,
        String md5,
        long length,
        boolean searchIndex
    ) {
        super(valueType, searchIndex);
        this.md5 = md5;
        this.length = length;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(Elasticsearch5Graph graph, long timestamp) {
        return graph.getStreamingPropertyValueService().read(this, timestamp);
    }

    public long getLength() {
        return length;
    }

    public String getMd5() {
        return md5;
    }
}
