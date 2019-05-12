package org.vertexium.inmemory.search;

import com.google.common.base.Joiner;
import org.vertexium.*;
import org.vertexium.search.MultiVertexQuery;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class DefaultMultiVertexQuery extends DefaultGraphQuery implements MultiVertexQuery {
    private final List<String> vertexIds;

    public DefaultMultiVertexQuery(Graph graph, String[] vertexIds, String queryString, User user) {
        super(graph, queryString, user);
        this.vertexIds = Arrays.asList(vertexIds);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T extends Element> Stream<T> getStreamFromElementType(ElementType elementType, FetchHints fetchHints) throws VertexiumException {
        switch (elementType) {
            case VERTEX:
                return (Stream<T>) getGraph().getVertices(vertexIds, fetchHints, getParameters().getUser());
            case EDGE:
                return (Stream<T>) getGraph().getVertices(vertexIds, fetchHints, getParameters().getUser())
                        .flatMap(v -> v.getEdges(Direction.BOTH, fetchHints, getParameters().getUser()));
            default:
                throw new VertexiumException("Unexpected element type: " + elementType);
        }
    }

    @Override
    public String toString() {
        return super.toString() +
            ", vertexIds=" + Joiner.on(", ").join(vertexIds);
    }
}
