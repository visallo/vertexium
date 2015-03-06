package org.neolumin.vertexium.accumulo;

import org.neolumin.vertexium.VertexiumException;
import org.neolumin.vertexium.property.StreamingPropertyValue;

import java.io.Serializable;

public abstract class StreamingPropertyValueRef implements Serializable {
    private static final long serialVersionUID = 1L;
    private String valueType;
    private boolean searchIndex;
    private boolean store;

    protected StreamingPropertyValueRef() {
        this.valueType = null;
        this.searchIndex = false;
        this.store = false;
    }

    protected StreamingPropertyValueRef(StreamingPropertyValue propertyValue) {
        this.valueType = propertyValue.getValueType().getName();
        this.searchIndex = propertyValue.isSearchIndex();
        this.store = propertyValue.isStore();
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

    public boolean isStore() {
        return store;
    }

    public abstract StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph);
}
