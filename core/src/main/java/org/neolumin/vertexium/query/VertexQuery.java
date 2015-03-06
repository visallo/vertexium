package org.neolumin.vertexium.query;

import org.neolumin.vertexium.Direction;
import org.neolumin.vertexium.Edge;
import org.neolumin.vertexium.FetchHint;

import java.util.EnumSet;

public interface VertexQuery extends Query {
    Iterable<Edge> edges(Direction direction);

    Iterable<Edge> edges(Direction direction, EnumSet<FetchHint> fetchHints);

    Iterable<Edge> edges(Direction direction, String label);

    Iterable<Edge> edges(Direction direction, String label, EnumSet<FetchHint> fetchHints);
}
