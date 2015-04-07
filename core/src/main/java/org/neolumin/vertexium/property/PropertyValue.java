package org.neolumin.vertexium.property;

import java.io.Serializable;

public class PropertyValue implements Serializable {
    static final long serialVersionUID = 42L;
    private boolean store = true;
    private boolean searchIndex = true;

    public PropertyValue() {

    }

    public PropertyValue store(boolean store) {
        this.store = store;
        return this;
    }

    public PropertyValue searchIndex(boolean searchIndex) {
        this.searchIndex = searchIndex;
        return this;
    }

    public boolean isStore() {
        return store;
    }

    public boolean isSearchIndex() {
        return searchIndex;
    }
}
