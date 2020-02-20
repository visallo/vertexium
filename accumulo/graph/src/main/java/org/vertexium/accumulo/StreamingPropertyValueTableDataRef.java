package org.vertexium.accumulo;

import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.property.StringStreamingPropertyValue;

public class StreamingPropertyValueTableDataRef extends StreamingPropertyValueRef<AccumuloGraph> {
    private static final long serialVersionUID = 576719549332292361L;
    private final String dataRowKey;
    private final Class valueType;
    private final Long length;
    private final transient String cachedValue;
    private final transient boolean dirty;

    // here for serialization
    protected StreamingPropertyValueTableDataRef() {
        dataRowKey = null;
        valueType = null;
        length = null;
        cachedValue = null;
        dirty = false;
    }

    public StreamingPropertyValueTableDataRef(String dataRowKey, StreamingPropertyValue propertyValue, long length) {
        super(propertyValue);
        this.dataRowKey = dataRowKey;
        this.valueType = propertyValue.getValueType();
        this.length = length;
        this.dirty = true;
        this.cachedValue = propertyValue instanceof StringStreamingPropertyValue ?
            propertyValue.readToString() :
            null;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph, long timestamp) {
        if (cachedValue != null) {
            return new StringStreamingPropertyValue(cachedValue);
        }
        return new StreamingPropertyValueTableData(graph, dataRowKey, valueType, length, timestamp, dirty);
    }
}
