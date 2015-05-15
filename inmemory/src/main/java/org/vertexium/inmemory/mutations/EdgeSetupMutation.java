package org.vertexium.inmemory.mutations;

import org.vertexium.Visibility;

public class EdgeSetupMutation extends Mutation {
    private final String outVertexId;
    private final String inVertexId;

    public EdgeSetupMutation(long timestamp, String outVertexId, String inVertexId) {
        super(timestamp, new Visibility(""));
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
    }

    public String getOutVertexId() {
        return outVertexId;
    }

    public String getInVertexId() {
        return inVertexId;
    }
}
