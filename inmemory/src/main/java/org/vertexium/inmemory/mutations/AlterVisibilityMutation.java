package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class AlterVisibilityMutation extends Mutation {
    private final Visibility newVisibility;

    public AlterVisibilityMutation(long timestamp, Visibility newVisibility) {
        super(timestamp, newVisibility);
        this.newVisibility = newVisibility;
    }

    public Visibility getNewVisibility() {
        return newVisibility;
    }
}
