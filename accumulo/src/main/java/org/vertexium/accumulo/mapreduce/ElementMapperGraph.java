package org.vertexium.accumulo.mapreduce;

import org.vertexium.*;
import org.vertexium.accumulo.AccumuloAuthorizations;
import org.vertexium.id.IdGenerator;
import org.vertexium.query.GraphQuery;
import org.vertexium.query.MultiVertexQuery;

import java.util.EnumSet;

public class ElementMapperGraph extends GraphBase {
    private ElementMapper elementMapper;

    public ElementMapperGraph(ElementMapper elementMapper) {
        this.elementMapper = elementMapper;
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility) {
        return this.elementMapper.prepareVertex(vertexId, timestamp, visibility);
    }

    @Override
    public Iterable<Vertex> getVertices(EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void deleteVertex(Vertex vertex, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void softDeleteEdge(Edge edge, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility) {
        return this.elementMapper.prepareEdge(edgeId, outVertex, inVertex, label, timestamp, visibility);
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility) {
        return this.elementMapper.prepareEdge(edgeId, outVertexId, inVertexId, label, timestamp, visibility);
    }

    @Override
    public Iterable<Edge> getEdges(EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void deleteEdge(Edge edge, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void setMetadata(String key, Object value) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public GraphQuery query(Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public GraphQuery query(String queryString, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public MultiVertexQuery query(String[] vertexIds, String queryString, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public MultiVertexQuery query(String[] vertexIds, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void reindex(Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void flush() {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void shutdown() {
        throw new VertexiumException("Not supported");
    }

    @Override
    public IdGenerator getIdGenerator() {
        return this.elementMapper.getIdGenerator();
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public DefinePropertyBuilder defineProperty(String propertyName) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public boolean isFieldBoostSupported() {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void clearData() {
        throw new VertexiumException("Not supported");
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations) {
        throw new VertexiumException("Not supported");
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        return new AccumuloAuthorizations(auths);
    }
}
