package org.vertexium.property;

import org.vertexium.Graph;
import org.vertexium.VertexiumException;

import java.io.Serializable;

public abstract class StreamingPropertyValueRef<T extends Graph> implements Serializable {
    private static final long serialVersionUID = 1L;
    private String valueType;
    private boolean searchIndex;
    private boolean store = true; // Need to keep for Kryo serialization

    protected StreamingPropertyValueRef() {
        this.valueType = null;
        this.searchIndex = false;
    }

    protected StreamingPropertyValueRef(StreamingPropertyValue propertyValue) {
        this(
            propertyValue.getValueType().getName(),
            propertyValue.isSearchIndex()
        );
    }

    protected StreamingPropertyValueRef(String valueType, boolean searchIndex) {
        this.valueType = valueType;
        this.searchIndex = searchIndex;
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
