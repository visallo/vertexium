package org.vertexium.cli.model;

import org.vertexium.FetchHint;
import org.vertexium.Vertex;

public class LazyVertexMap extends ModelBase {
    public LazyVertex get(String vertexId) {
        Vertex v = getGraph().getVertex(vertexId, FetchHint.ALL, getTime(), getAuthorizations());
        if (v == null) {
            return null;
        }
        return new LazyVertex(vertexId);
    }
}
