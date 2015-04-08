package org.vertexium.query;

import org.vertexium.Direction;
import org.vertexium.Edge;
import org.vertexium.FetchHint;

import java.util.EnumSet;

public interface VertexQuery extends Query {
    Iterable<Edge> edges(Direction direction);

    Iterable<Edge> edges(Direction direction, EnumSet<FetchHint> fetchHints);

    Iterable<Edge> edges(Direction direction, String label);

    Iterable<Edge> edges(Direction direction, String label, EnumSet<FetchHint> fetchHints);
}
