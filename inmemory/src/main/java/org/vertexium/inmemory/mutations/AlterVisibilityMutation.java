package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class AlterVisibilityMutation extends Mutation {
    private final Visibility newVisibility;
    private final Object data;

    public AlterVisibilityMutation(long timestamp, Visibility newVisibility, Object data) {
        super(timestamp, newVisibility);
        this.newVisibility = newVisibility;
        this.data = data;
    }

    public Visibility getNewVisibility() {
        return newVisibility;
    }

    public Object getData() {
        return data;
    }
}
