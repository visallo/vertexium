package org.neolumin.vertexium.cli.model;

import org.neolumin.vertexium.Edge;

public class LazyEdgeMap extends ModelBase {
    public LazyEdge get(String edgeId) {
        Edge e = getGraph().getEdge(edgeId, getAuthorizations());
        if (e == null) {
            return null;
        }
        return new LazyEdge(edgeId);
    }
}
