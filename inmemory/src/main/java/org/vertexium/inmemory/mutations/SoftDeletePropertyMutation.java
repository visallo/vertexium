package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class SoftDeletePropertyMutation extends PropertyMutation {
    private final Object data;

    public SoftDeletePropertyMutation(long timestamp, String key, String name, Visibility propertyVisibility, Object data) {
        super(timestamp, key, name, propertyVisibility, new Visibility(""));
        this.data = data;
    }

    public Object getData() {
        return data;
    }
}
