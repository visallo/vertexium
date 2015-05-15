package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class SoftDeletePropertyMutation extends PropertyMutation {
    public SoftDeletePropertyMutation(long timestamp, String key, String name, Visibility propertyVisibility) {
        super(timestamp, key, name, propertyVisibility, new Visibility(""));
    }
}
