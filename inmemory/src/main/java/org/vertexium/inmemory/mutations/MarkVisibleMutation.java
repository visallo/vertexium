package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class MarkVisibleMutation extends Mutation {
    public MarkVisibleMutation(long timestamp, Visibility visibility) {
        super(timestamp, visibility);
    }
}
