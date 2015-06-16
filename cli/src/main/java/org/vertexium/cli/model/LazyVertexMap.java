package org.vertexium.cli.model;

import org.vertexium.FetchHint;
import org.vertexium.Vertex;

import java.util.ArrayList;
import java.util.List;

public class LazyVertexMap extends ModelBase {
    public Object get(String vertexId) {
        if (vertexId.endsWith("*")) {
            String vertexIdPrefix = vertexId.substring(0, vertexId.length() - 1);
            Iterable<Vertex> vertices = getGraph().getVerticesWithPrefix(vertexIdPrefix, FetchHint.ALL, getTime(), getAuthorizations());
            List<String> results = new ArrayList<>();
            for (Vertex v : vertices) {
                results.add(v.getId());
            }
            return new LazyVertexList(results);
        } else {
            Vertex v = getGraph().getVertex(vertexId, FetchHint.ALL, getTime(), getAuthorizations());
            if (v == null) {
                return null;
            }
            return new LazyVertex(vertexId);
        }
    }
}
