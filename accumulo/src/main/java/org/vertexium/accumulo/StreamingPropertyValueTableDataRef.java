package org.vertexium.accumulo;

import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

public class StreamingPropertyValueTableDataRef extends StreamingPropertyValueRef<AccumuloGraph> {
    private static final long serialVersionUID = 576719549332292361L;
    private final String dataRowKey;
    private final Class valueType;
    private final Long length;

    // here for serialization
    protected StreamingPropertyValueTableDataRef() {
        dataRowKey = null;
        valueType = null;
        length = null;
    }

    public StreamingPropertyValueTableDataRef(String dataRowKey, StreamingPropertyValue propertyValue, long length) {
        super(propertyValue);
        this.dataRowKey = dataRowKey;
        this.valueType = propertyValue.getValueType();
        this.length = length;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph, long timestamp) {
        return new StreamingPropertyValueTableData(graph, dataRowKey, valueType, length, timestamp);
    }
}
