package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class ElementTimestampMutation extends Mutation {
    public ElementTimestampMutation(long timestamp) {
        super(timestamp, new Visibility(""));
    }
}
