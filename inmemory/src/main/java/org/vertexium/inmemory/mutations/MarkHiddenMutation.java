package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class MarkHiddenMutation extends Mutation {
    public MarkHiddenMutation(long timestamp, Visibility visibility) {
        super(timestamp, visibility);
    }
}
