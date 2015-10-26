package org.vertexium.accumulo;

import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

public class StreamingPropertyValueTableRef extends StreamingPropertyValueRef<AccumuloGraph> {
    private final String dataRowKey;
    private transient byte[] data;

    // here for serialization
    protected StreamingPropertyValueTableRef() {
        this.dataRowKey = null;
    }

    public StreamingPropertyValueTableRef(String dataRowKey, StreamingPropertyValue propertyValue, byte[] data) {
        super(propertyValue);
        this.dataRowKey = dataRowKey;
        this.data = data;
    }

    public String getDataRowKey() {
        return dataRowKey;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph) {
        return new StreamingPropertyValueTable(graph, getDataRowKey(), this);
    }
}
