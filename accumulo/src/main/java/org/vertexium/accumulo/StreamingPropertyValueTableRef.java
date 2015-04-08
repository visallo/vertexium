package org.vertexium.accumulo;

import org.vertexium.property.StreamingPropertyValue;

public class StreamingPropertyValueTableRef extends StreamingPropertyValueRef {
    private String dataRowKey;
    private transient byte[] data;

    protected StreamingPropertyValueTableRef() {
        super();
        dataRowKey = null;
        data = null;
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
