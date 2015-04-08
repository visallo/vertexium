package org.vertexium.inmemory.util;

import org.vertexium.Edge;
import org.vertexium.util.ConvertingIterable;

public class EdgeToEdgeIdIterable extends ConvertingIterable<Edge, String> {
    public EdgeToEdgeIdIterable(Iterable<Edge> edges) {
        super(edges);
    }

    @Override
    protected String convert(Edge edge) {
        return edge.getId();
    }
}
