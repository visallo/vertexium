package org.vertexium.property;

import java.io.Serializable;

public class PropertyValue implements Serializable {
    static final long serialVersionUID = 42L;
    private boolean store = true; // Need to keep for Kryo serialization
    private boolean searchIndex = true;

    public PropertyValue() {

    }

    @SuppressWarnings("unchecked")
    public <T extends PropertyValue> T searchIndex(boolean searchIndex) {
        this.searchIndex = searchIndex;
        return (T) this;
    }

    public boolean isSearchIndex() {
        return searchIndex;
    }
}
