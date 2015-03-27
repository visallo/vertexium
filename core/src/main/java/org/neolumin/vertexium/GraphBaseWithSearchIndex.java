package org.neolumin.vertexium;

import org.neolumin.vertexium.id.IdGenerator;
import org.neolumin.vertexium.query.GraphQuery;
import org.neolumin.vertexium.query.MultiVertexQuery;
import org.neolumin.vertexium.query.SimilarToGraphQuery;
import org.neolumin.vertexium.search.SearchIndex;
import org.neolumin.vertexium.util.ToElementIterable;

import java.io.IOException;

public abstract class GraphBaseWithSearchIndex extends GraphBase implements Graph {
    public static final String METADATA_DEFINE_PROPERTY_PREFIX = "defineProperty.";
    public static final String METADATA_ID_GENERATOR_CLASSNAME = "idGenerator.classname";
    private final GraphConfiguration configuration;
    private final IdGenerator idGenerator;
    private SearchIndex searchIndex;
    private boolean foundIdGeneratorClassnameInMetadata;

    protected GraphBaseWithSearchIndex(GraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        this.configuration = configuration;
        this.idGenerator = idGenerator;
        this.searchIndex = searchIndex;
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
                setupPropertyDefinition((PropertyDefinition) graphMetadataEntry.getValue());
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

    protected void setupPropertyDefinition(PropertyDefinition propertyDefinition) {
        try {
            getSearchIndex().addPropertyDefinition(propertyDefinition);
        } catch (IOException e) {
            throw new VertexiumException("Could not add property definition to search index", e);
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

    public SearchIndex getSearchIndex() {
        return searchIndex;
    }

    @Override
    public void reindex(Authorizations authorizations) {
        reindexVertices(authorizations);
        reindexEdges(authorizations);
    }

    protected void reindexVertices(Authorizations authorizations) {
        this.searchIndex.addElements(this, new ToElementIterable<>(getVertices(authorizations)), authorizations);
    }

    private void reindexEdges(Authorizations authorizations) {
        this.searchIndex.addElements(this, new ToElementIterable<>(getEdges(authorizations)), authorizations);
    }

    @Override
    public void flush() {
        if (getSearchIndex() != null) {
            this.searchIndex.flush();
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
    public DefinePropertyBuilder defineProperty(final String propertyName) {
        return new DefinePropertyBuilder(propertyName) {
            @Override
            public PropertyDefinition define() {
                PropertyDefinition propertyDefinition = super.define();
                try {
                    getSearchIndex().addPropertyDefinition(propertyDefinition);
                } catch (IOException e) {
                    throw new VertexiumException("Could not add property definition to search index", e);
                }
                setMetadata(METADATA_DEFINE_PROPERTY_PREFIX + propertyName, propertyDefinition);
                return propertyDefinition;
            }
        };
    }

    @Override
    public boolean isFieldBoostSupported() {
        return getSearchIndex().isFieldBoostSupported();
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return getSearchIndex().getSearchIndexSecurityGranularity();
    }
}
