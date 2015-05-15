package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class SoftDeleteMutation extends Mutation {
    public SoftDeleteMutation(long timestamp) {
        super(timestamp, new Visibility(""));
    }
}
