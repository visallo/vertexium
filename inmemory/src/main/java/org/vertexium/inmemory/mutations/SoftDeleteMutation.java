package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class SoftDeleteMutation extends Mutation {
    private final Object data;

    public SoftDeleteMutation(long timestamp, Object data) {
        super(timestamp, new Visibility(""));
        this.data = data;
    }

    public Object getData() {
        return data;
    }
}
