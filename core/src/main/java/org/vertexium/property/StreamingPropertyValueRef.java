package org.vertexium.property;

import org.vertexium.Graph;
import org.vertexium.VertexiumException;

import java.io.Serializable;

public abstract class StreamingPropertyValueRef<T extends Graph> implements Serializable {
    private static final long serialVersionUID = 1L;
    private String valueType;
    private boolean searchIndex;

    protected StreamingPropertyValueRef() {
        this.valueType = null;
        this.searchIndex = false;
    }

    protected StreamingPropertyValueRef(StreamingPropertyValue propertyValue) {
        this.valueType = propertyValue.getValueType().getName();
        this.searchIndex = propertyValue.isSearchIndex();
    }

    public Class getValueType() {
        try {
            return Class.forName(valueType);
        } catch (ClassNotFoundException e) {
            throw new VertexiumException("Could not get type: " + valueType);
        }
    }

    public boolean isSearchIndex() {
        return searchIndex;
    }

    public abstract StreamingPropertyValue toStreamingPropertyValue(T graph, long timestamp);
}
