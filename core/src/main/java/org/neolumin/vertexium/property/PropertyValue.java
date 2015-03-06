package org.neolumin.vertexium.property;

public class PropertyValue {
    private boolean store = true;
    private boolean searchIndex = true;

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
