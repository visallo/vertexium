package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class AlterEdgeLabelMutation extends Mutation {
    private final String newEdgeLabel;

    public AlterEdgeLabelMutation(long timestamp, String newEdgeLabel) {
        super(timestamp, new Visibility(""));
        this.newEdgeLabel = newEdgeLabel;
    }

    public String getNewEdgeLabel() {
        return newEdgeLabel;
    }
}
