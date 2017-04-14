package org.vertexium.cli.model;

import org.vertexium.Edge;
import org.vertexium.FetchHint;

public class LazyEdgeMap extends ModelBase {
    public LazyEdge get(String edgeId) {
        Edge e = getGraph().getEdge(edgeId, FetchHint.DEFAULT, getTime(), getAuthorizations());
        if (e == null) {
            return null;
        }
        return new LazyEdge(edgeId);
    }
}
