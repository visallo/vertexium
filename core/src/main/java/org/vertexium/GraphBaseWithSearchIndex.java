package org.vertexium;

import org.vertexium.id.IdGenerator;
import org.vertexium.query.GraphQuery;
import org.vertexium.query.MultiVertexQuery;
import org.vertexium.query.SimilarToGraphQuery;
import org.vertexium.search.SearchIndex;
import org.vertexium.search.SearchIndexWithVertexPropertyCountByValue;

import java.util.EnumSet;
import java.util.Map;

public abstract class GraphBaseWithSearchIndex extends GraphBase implements Graph, GraphWithSearchIndex {
    public static final String METADATA_ID_GENERATOR_CLASSNAME = "idGenerator.classname";
    private final GraphConfiguration configuration;
    private final IdGenerator idGenerator;
    private SearchIndex searchIndex;
    private boolean foundIdGeneratorClassnameInMetadata;

    protected GraphBaseWithSearchIndex(GraphConfiguration configuration) {
        super(configuration.isStrictTyping());
        this.configuration = configuration;
        this.searchIndex = configuration.createSearchIndex(this);
        this.idGenerator = configuration.createIdGenerator(this);
    }

    protected GraphBaseWithSearchIndex(GraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        super(configuration.isStrictTyping());
        this.configuration = configuration;
        this.searchIndex = searchIndex;
        this.idGenerator = idGenerator;
    }

    protected void setup() {
        setupGraphMetadata();
    }

    protected void setupGraphMetadata() {
        foundIdGeneratorClassnameInMetadata = false;
        for (GraphMetadataEntry graphMetadataEntry : getMetadata()) {
            setupGraphMetadata(graphMetadataEntry);
        }
        if (!foundIdGeneratorClassnameInMetadata) {
            setMetadata(METADATA_ID_GENERATOR_CLASSNAME, this.idGenerator.getClass().getName());
        }
    }

    protected void setupGraphMetadata(GraphMetadataEntry graphMetadataEntry) {
        if (graphMetadataEntry.getKey().startsWith(METADATA_DEFINE_PROPERTY_PREFIX)) {
            if (graphMetadataEntry.getValue() instanceof PropertyDefinition) {
                addToPropertyDefinitionCache((PropertyDefinition) graphMetadataEntry.getValue());
            } else {
                throw new VertexiumException("Invalid property definition metadata: " + graphMetadataEntry.getKey() + " expected " + PropertyDefinition.class.getName() + " found " + graphMetadataEntry.getValue().getClass().getName());
            }
        } else if (graphMetadataEntry.getKey().equals(METADATA_ID_GENERATOR_CLASSNAME)) {
            if (graphMetadataEntry.getValue() instanceof String) {
                String idGeneratorClassname = (String) graphMetadataEntry.getValue();
                if (idGeneratorClassname.equals(idGenerator.getClass().getName())) {
                    foundIdGeneratorClassnameInMetadata = true;
                }
            } else {
                throw new VertexiumException("Invalid " + METADATA_ID_GENERATOR_CLASSNAME + " expected String found " + graphMetadataEntry.getValue().getClass().getName());
            }
        }
    }

    @Override
    public GraphQuery query(Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, null, authorizations);
    }

    @Override
    public GraphQuery query(String queryString, Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, queryString, authorizations);
    }

    @Override
    public MultiVertexQuery query(String[] vertexIds, String queryString, Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, vertexIds, queryString, authorizations);
    }

    @Override
    public MultiVertexQuery query(String[] vertexIds, Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, vertexIds, null, authorizations);
    }

    @Override
    public boolean isQuerySimilarToTextSupported() {
        return getSearchIndex().isQuerySimilarToTextSupported();
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(String[] fields, String text, Authorizations authorizations) {
        return getSearchIndex().querySimilarTo(this, fields, text, authorizations);
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public GraphConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public SearchIndex getSearchIndex() {
        return searchIndex;
    }

    @Override
    public void reindex(Authorizations authorizations) {
        reindexVertices(authorizations);
        reindexEdges(authorizations);
    }

    protected void reindexVertices(Authorizations authorizations) {
        this.searchIndex.addElements(this, getVertices(authorizations), authorizations);
    }

    private void reindexEdges(Authorizations authorizations) {
        this.searchIndex.addElements(this, getEdges(authorizations), authorizations);
    }

    @Override
    public void flush() {
        if (getSearchIndex() != null) {
            this.searchIndex.flush(this);
        }
    }

    @Override
    public void shutdown() {
        flush();
        if (getSearchIndex() != null) {
            this.searchIndex.shutdown();
            this.searchIndex = null;
        }
    }

    @Override
    public abstract void drop();

    @Override
    public boolean isFieldBoostSupported() {
        return getSearchIndex().isFieldBoostSupported();
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return getSearchIndex().getSearchIndexSecurityGranularity();
    }

    @Override
    public Map<Object, Long> getVertexPropertyCountByValue(String propertyName, Authorizations authorizations) {
        if (getSearchIndex() instanceof SearchIndexWithVertexPropertyCountByValue) {
            return ((SearchIndexWithVertexPropertyCountByValue) getSearchIndex()).getVertexPropertyCountByValue(this, propertyName, authorizations);
        }
        return super.getVertexPropertyCountByValue(propertyName, authorizations);
    }

    @Override
    public abstract VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility);

    @Override
    public abstract Iterable<Vertex> getVertices(EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations);

    @Override
    public abstract EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility);

    @Override
    public abstract EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility);

    @Override
    public abstract void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations);

    @Override
    public abstract void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations);

    @Override
    public abstract Iterable<Edge> getEdges(EnumSet<FetchHint> fetchHints, Long endTime, Authorizations authorizations);

    @Override
    protected abstract GraphMetadataStore getGraphMetadataStore();

    @Override
    public abstract void deleteVertex(Vertex vertex, Authorizations authorizations);

    @Override
    public abstract void deleteEdge(Edge edge, Authorizations authorizations);

    @Override
    public abstract boolean isVisibilityValid(Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void truncate();

    @Override
    public abstract void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract Authorizations createAuthorizations(String... auths);
}
