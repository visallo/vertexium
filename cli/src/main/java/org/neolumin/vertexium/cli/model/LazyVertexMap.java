package org.neolumin.vertexium.cli.model;

import org.neolumin.vertexium.Vertex;

public class LazyVertexMap extends ModelBase {
    public LazyVertex get(String vertexId) {
        Vertex v = getGraph().getVertex(vertexId, getAuthorizations());
        if (v == null) {
            return null;
        }
        return new LazyVertex(vertexId);
    }
}
