package org.vertexium.blueprints;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import org.vertexium.Authorizations;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.LookAheadIterable;

import java.util.*;

import static org.vertexium.util.IterableUtils.toList;

public class VertexiumBlueprintsVertex extends VertexiumBlueprintsElement implements Vertex {
    protected VertexiumBlueprintsVertex(VertexiumBlueprintsGraph graph, org.vertexium.Vertex vertex, Authorizations authorizations) {
        super(graph, vertex, authorizations);
    }

    public static Vertex create(VertexiumBlueprintsGraph graph, org.vertexium.Vertex vertex, Authorizations authorizations) {
        if (vertex == null) {
            return null;
        }
        return new VertexiumBlueprintsVertex(graph, vertex, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, final String... labels) {
        final org.vertexium.Direction sgDirection = VertexiumBlueprintsConvert.toVertexium(direction);
        final Authorizations authorizations = getGraph().getAuthorizationsProvider().getAuthorizations();
        final List<org.vertexium.Edge> vertexiumEdges = toList(getVertexiumElement().getEdges(sgDirection, labels, authorizations));
        final Set<String> visibleVertexIds = getVisibleVertexIds(vertexiumEdges, authorizations);
        return new LookAheadIterable<org.vertexium.Edge, Edge>() {
            @Override
            protected boolean isIncluded(org.vertexium.Edge src, Edge edge) {
                return edge != null;
            }

            @Override
            protected Edge convert(org.vertexium.Edge vertexiumEdge) {
                if (!canSeeBothVertices(vertexiumEdge)) {
                    return null;
                }
                return VertexiumBlueprintsEdge.create(getGraph(), vertexiumEdge, authorizations);
            }

            private boolean canSeeBothVertices(org.vertexium.Edge vertexiumEdge) {
                return visibleVertexIds.contains(vertexiumEdge.getVertexId(org.vertexium.Direction.OUT))
                        && visibleVertexIds.contains(vertexiumEdge.getVertexId(org.vertexium.Direction.IN));
            }

            @Override
            protected Iterator<org.vertexium.Edge> createIterator() {
                return vertexiumEdges.iterator();
            }
        };
    }

    private Set<String> getVisibleVertexIds(List<org.vertexium.Edge> edges, Authorizations authorizations) {
        Set<String> results = new HashSet<>();
        for (org.vertexium.Edge edge : edges) {
            results.add(edge.getVertexId(org.vertexium.Direction.IN));
            results.add(edge.getVertexId(org.vertexium.Direction.OUT));
        }
        Map<String, Boolean> exists = getGraph().getGraph().doVerticesExist(results, authorizations);
        for (Map.Entry<String, Boolean> exist : exists.entrySet()) {
            if (!exist.getValue()) {
                results.remove(exist.getKey());
            }
        }
        return results;
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, final String... labels) {
        final org.vertexium.Direction sgDirection = VertexiumBlueprintsConvert.toVertexium(direction);
        final Authorizations authorizations = getGraph().getAuthorizationsProvider().getAuthorizations();
        return new ConvertingIterable<org.vertexium.Vertex, Vertex>(getVertexiumElement().getVertices(sgDirection, labels, authorizations)) {
            @Override
            protected Vertex convert(org.vertexium.Vertex vertex) {
                return VertexiumBlueprintsVertex.create(getGraph(), vertex, authorizations);
            }
        };
    }

    @Override
    public VertexQuery query() {
        return new DefaultVertexQuery(this); // TODO implement
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        if (label == null) {
            throw new IllegalArgumentException("Cannot add edge with null label");
        }
        return getGraph().addEdge(null, this, inVertex, label);
    }

    @Override
    public void remove() {
        getGraph().removeVertex(this);
    }

    @Override
    public org.vertexium.Vertex getVertexiumElement() {
        return (org.vertexium.Vertex) super.getVertexiumElement();
    }
}
