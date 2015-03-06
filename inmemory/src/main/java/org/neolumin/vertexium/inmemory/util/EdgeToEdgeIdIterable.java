package org.neolumin.vertexium.inmemory.util;

import org.neolumin.vertexium.Edge;
import org.neolumin.vertexium.util.ConvertingIterable;

public class EdgeToEdgeIdIterable extends ConvertingIterable<Edge, String> {
    public EdgeToEdgeIdIterable(Iterable<Edge> edges) {
        super(edges);
    }

    @Override
    protected String convert(Edge edge) {
        return edge.getId();
    }
}
