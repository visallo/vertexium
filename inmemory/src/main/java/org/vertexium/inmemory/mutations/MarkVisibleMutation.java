package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class MarkVisibleMutation extends Mutation {
    private final Object data;

    public MarkVisibleMutation(long timestamp, Visibility visibility, Object data) {
        super(timestamp, visibility);
        this.data = data;
    }

    public Object getData() {
        return data;
    }
}
