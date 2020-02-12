package org.vertexium.accumulo;

import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.property.StringStreamingPropertyValue;

public class StringStreamingPropertyValueRef extends StreamingPropertyValueRef<AccumuloGraph> {
    StringStreamingPropertyValue propertyValue;

    public StringStreamingPropertyValueRef(StringStreamingPropertyValue propertyValue) {
        super(propertyValue);
        this.propertyValue = propertyValue;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph, long timestamp) {
        return propertyValue;
    }
}
