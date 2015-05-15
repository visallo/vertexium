package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class MarkPropertyHiddenMutation extends PropertyMutation {
    public MarkPropertyHiddenMutation(String propertyKey, String propertyName, Visibility propertyVisibility, long timestamp, Visibility hiddenVisibility) {
        super(timestamp, propertyKey, propertyName, propertyVisibility, hiddenVisibility);
    }
}
