package org.neolumin.vertexium.blueprints;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.Graph;
import org.neolumin.vertexium.Visibility;
import org.neolumin.vertexium.query.Compare;
import org.neolumin.vertexium.util.ConvertingIterable;

public abstract class VertexiumBlueprintsGraph implements com.tinkerpop.blueprints.Graph {
    private static final VertexiumBlueprintsGraphFeatures FEATURES = new VertexiumBlueprintsGraphFeatures();
    private final Graph vertexiumGraph;
    private final VisibilityProvider visibilityProvider;
    private final AuthorizationsProvider authorizationsProvider;

    protected VertexiumBlueprintsGraph(Graph vertexiumGraph, VisibilityProvider visibilityProvider, AuthorizationsProvider authorizationsProvider) {
        this.vertexiumGraph = vertexiumGraph;
        this.visibilityProvider = visibilityProvider;
        this.authorizationsProvider = authorizationsProvider;
    }

    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    @Override
    public Vertex addVertex(Object id) {
        Visibility visibility = getVisibilityProvider().getVisibilityForVertex(VertexiumBlueprintsConvert.idToString(id));
        Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return VertexiumBlueprintsVertex.create(this, getGraph().addVertex(VertexiumBlueprintsConvert.idToString(id), visibility, authorizations), authorizations);
    }

    @Override
    public Vertex getVertex(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null");
        }
        Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return VertexiumBlueprintsVertex.create(this, getGraph().getVertex(VertexiumBlueprintsConvert.idToString(id), authorizations), authorizations);
    }

    @Override
    public void removeVertex(Vertex vertex) {
        org.neolumin.vertexium.Vertex sgVertex = VertexiumBlueprintsConvert.toVertexium(vertex);
        getGraph().removeVertex(sgVertex, getAuthorizationsProvider().getAuthorizations());
    }

    @Override
    public Iterable<Vertex> getVertices() {
        final Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return new ConvertingIterable<org.neolumin.vertexium.Vertex, Vertex>(getGraph().getVertices(authorizations)) {
            @Override
            protected Vertex convert(org.neolumin.vertexium.Vertex vertex) {
                return VertexiumBlueprintsVertex.create(VertexiumBlueprintsGraph.this, vertex, authorizations);
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(final String key, final Object value) {
        final Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return new ConvertingIterable<org.neolumin.vertexium.Vertex, Vertex>(getGraph().query(authorizations).has(key, Compare.EQUAL, value).vertices()) {
            @Override
            protected Vertex convert(org.neolumin.vertexium.Vertex vertex) {
                return VertexiumBlueprintsVertex.create(VertexiumBlueprintsGraph.this, vertex, authorizations);
            }
        };

    }

    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        if (label == null) {
            throw new IllegalArgumentException("label cannot be null");
        }
        org.neolumin.vertexium.Vertex sgOutVertex = VertexiumBlueprintsConvert.toVertexium(outVertex);
        org.neolumin.vertexium.Vertex sgInVertex = VertexiumBlueprintsConvert.toVertexium(inVertex);
        Visibility visibility = getVisibilityProvider().getVisibilityForEdge(VertexiumBlueprintsConvert.idToString(id), sgOutVertex, sgInVertex, label);
        Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return VertexiumBlueprintsEdge.create(this, getGraph().addEdge(VertexiumBlueprintsConvert.idToString(id), sgOutVertex, sgInVertex, label, visibility, authorizations), authorizations);
    }

    @Override
    public Edge getEdge(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null");
        }
        Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return VertexiumBlueprintsEdge.create(this, getGraph().getEdge(VertexiumBlueprintsConvert.idToString(id), authorizations), authorizations);
    }

    @Override
    public void removeEdge(Edge edge) {
        org.neolumin.vertexium.Edge sgEdge = VertexiumBlueprintsConvert.toVertexium(edge);
        getGraph().removeEdge(sgEdge, getAuthorizationsProvider().getAuthorizations());
    }

    @Override
    public Iterable<Edge> getEdges() {
        final Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return new ConvertingIterable<org.neolumin.vertexium.Edge, Edge>(getGraph().getEdges(authorizations)) {
            @Override
            protected Edge convert(org.neolumin.vertexium.Edge edge) {
                return VertexiumBlueprintsEdge.create(VertexiumBlueprintsGraph.this, edge, authorizations);
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final String key, final Object value) {
        final Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return new ConvertingIterable<org.neolumin.vertexium.Edge, Edge>(getGraph().query(authorizations).has(key, Compare.EQUAL, value).edges()) {
            @Override
            protected Edge convert(org.neolumin.vertexium.Edge edge) {
                return VertexiumBlueprintsEdge.create(VertexiumBlueprintsGraph.this, edge, authorizations);
            }
        };
    }

    @Override
    public GraphQuery query() {
        return new DefaultGraphQuery(this); // TODO implement this
    }

    @Override
    public void shutdown() {
        getGraph().shutdown();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName().toLowerCase();
    }

    public Graph getGraph() {
        return vertexiumGraph;
    }

    public VisibilityProvider getVisibilityProvider() {
        return visibilityProvider;
    }

    public AuthorizationsProvider getAuthorizationsProvider() {
        return authorizationsProvider;
    }
}
