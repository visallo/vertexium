package org.vertexium.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.vertexium.*;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.query.GraphQuery;
import org.vertexium.query.SimilarToGraphQuery;
import org.vertexium.query.VertexQuery;
import org.vertexium.type.GeoCircle;
import org.vertexium.type.GeoPoint;
import org.vertexium.util.StreamUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchSearchIndex extends ElasticSearchSearchIndexBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticSearchSearchIndex.class);
    private NameSubstitutionStrategy nameSubstitutionStrategy;

    public ElasticSearchSearchIndex(Graph graph, GraphConfiguration config) {
        super(graph, config);
        this.nameSubstitutionStrategy = getConfig().getNameSubstitutionStrategy();
    }

    @Override
    protected boolean isStoreSourceData() {
        return true;
    }

    @Override
    public void addElement(Graph graph, Element element, Authorizations authorizations) {
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("addElement: %s", element.getId());
        }
        if (!getConfig().isIndexEdges() && element instanceof Edge) {
            return;
        }

        IndexInfo indexInfo = addPropertiesToIndex(graph, element, element.getProperties());

        try {
            XContentBuilder jsonBuilder = buildJsonContentFromElement(graph, indexInfo, element, authorizations);
            XContentBuilder source = jsonBuilder.endObject();
            if (MUTATION_LOGGER.isTraceEnabled()) {
                MUTATION_LOGGER.trace("addElement json: %s: %s", element.getId(), source.string());
            }

            UpdateResponse response = getClient()
                    .prepareUpdate(indexInfo.getIndexName(), ELEMENT_TYPE, element.getId())
                    .setDocAsUpsert(true)
                    .setDoc(source)
                    .execute()
                    .actionGet();
            if (response.getId() == null) {
                throw new VertexiumException("Could not index document " + element.getId());
            }

            if (getConfig().isAutoFlush()) {
                flush();
            }
        } catch (Exception e) {
            throw new VertexiumException("Could not add element", e);
        }

        getConfig().getScoringStrategy().addElement(this, graph, element, authorizations);
    }

    @Override
    public void addElementToBulkRequest(Graph graph, BulkRequest bulkRequest, IndexInfo indexInfo, Element element, Authorizations authorizations) {
        try {
            XContentBuilder json = buildJsonContentFromElement(graph, indexInfo, element, authorizations);
            UpdateRequest indexRequest = new UpdateRequest(indexInfo.getIndexName(), ELEMENT_TYPE, element.getId()).doc(json);
            indexRequest.docAsUpsert(true);
            bulkRequest.add(indexRequest);
        } catch (IOException ex) {
            throw new VertexiumException("Could not add element to bulk request", ex);
        }
    }

    @Override
    public void deleteElement(Graph graph, Element element, Authorizations authorizations) {
        String indexName = getIndexName(element);
        String id = element.getId();
        if (MUTATION_LOGGER.isTraceEnabled()) {
            LOGGER.trace("deleting document %s", id);
        }
        DeleteResponse deleteResponse = getClient().delete(
                getClient()
                        .prepareDelete(indexName, ELEMENT_TYPE, id)
                        .request()
        ).actionGet();
        if (!deleteResponse.isFound()) {
            throw new VertexiumException("Could not delete element " + element.getId());
        }
    }

    String createJsonForElement(Graph graph, Element element, Authorizations authorizations) {
        try {
            String indexName = getIndexName(element);
            IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName, isStoreSourceData());
            return buildJsonContentFromElement(graph, indexInfo, element, authorizations).string();
        } catch (Exception e) {
            throw new VertexiumException("Could not create JSON for element", e);
        }
    }

    private XContentBuilder buildJsonContentFromElement(Graph graph, IndexInfo indexInfo, Element element, Authorizations authorizations) throws IOException {
        XContentBuilder jsonBuilder;
        jsonBuilder = XContentFactory.jsonBuilder()
                .startObject();

        if (element instanceof Vertex) {
            jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, ElasticSearchElementType.VERTEX.getKey());
            getConfig().getScoringStrategy().addFieldsToVertexDocument(this, jsonBuilder, (Vertex) element, null, authorizations);
        } else if (element instanceof Edge) {
            jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, ElasticSearchElementType.EDGE.getKey());
            getConfig().getScoringStrategy().addFieldsToEdgeDocument(this, jsonBuilder, (Edge) element, null, authorizations);
        } else {
            throw new VertexiumException("Unexpected element type " + element.getClass().getName());
        }

        Map<String, Object> properties = getProperties(graph, element, indexInfo);
        for (Map.Entry<String, Object> property : properties.entrySet()) {
            if (property.getValue() instanceof List) {
                List list = (List) property.getValue();
                jsonBuilder.field(property.getKey(), list.toArray(new Object[list.size()]));
            } else {
                jsonBuilder.field(property.getKey(), property.getValue());
            }
        }

        return jsonBuilder;
    }

    private Map<String, Object> getProperties(Graph graph, Element element, IndexInfo indexInfo) throws IOException {
        Map<String, Object> propertiesMap = new HashMap<>();
        for (Property property : element.getProperties()) {
            Object propertyValue = property.getValue();
            String propertyName = this.nameSubstitutionStrategy.deflate(property.getName());
            if (propertyValue != null && shouldIgnoreType(propertyValue.getClass())) {
                continue;
            } else if (propertyValue instanceof GeoPoint) {
                convertGeoPoint(graph, propertiesMap, property, (GeoPoint) propertyValue);
                continue;
            } else if (propertyValue instanceof GeoCircle) {
                convertGeoCircle(graph, propertiesMap, property, (GeoCircle) propertyValue);
                continue;
            } else if (propertyValue instanceof StreamingPropertyValue) {
                StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValue;
                if (!streamingPropertyValue.isSearchIndex()) {
                    continue;
                }

                PropertyDefinition propertyDefinition = indexInfo.getPropertyDefinitions().get(propertyName);
                if (propertyDefinition != null && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                    continue;
                }

                Class valueType = streamingPropertyValue.getValueType();
                if (valueType == String.class) {
                    InputStream in = streamingPropertyValue.getInputStream();
                    propertyValue = StreamUtils.toString(in);
                } else {
                    throw new VertexiumException("Unhandled StreamingPropertyValue type: " + valueType.getName());
                }
            } else if (propertyValue instanceof String) {
                PropertyDefinition propertyDefinition = indexInfo.getPropertyDefinitions().get(propertyName);
                if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                    addPropertyValueToPropertiesMap(propertiesMap, propertyName + EXACT_MATCH_PROPERTY_NAME_SUFFIX, propertyValue);
                }
                if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                    addPropertyValueToPropertiesMap(propertiesMap, propertyName, propertyValue);
                }
                continue;
            }

            if (propertyValue instanceof DateOnly) {
                propertyValue = ((DateOnly) propertyValue).getDate();
            }

            addPropertyValueToPropertiesMap(propertiesMap, propertyName, propertyValue);
        }
        return propertiesMap;
    }

    @Override
    protected void addPropertyToIndex(Graph graph, IndexInfo indexInfo, String propertyName, Class dataType, boolean analyzed, Double boost) throws IOException {
        if (indexInfo.isPropertyDefined(propertyName)) {
            return;
        }

        if (shouldIgnoreType(dataType)) {
            return;
        }

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(ELEMENT_TYPE)
                .startObject("properties")
                .startObject(propertyName);

        addTypeToMapping(mapping, propertyName, dataType, analyzed, boost);

        mapping
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("addPropertyToIndex: %s: %s", dataType.getName(), mapping.string());
        }

        getClient()
                .admin()
                .indices()
                .preparePutMapping(indexInfo.getIndexName())
                .setIgnoreConflicts(false)
                .setType(ELEMENT_TYPE)
                .setSource(mapping)
                .execute()
                .actionGet();

        indexInfo.addPropertyDefinition(propertyName, new PropertyDefinition(propertyName, dataType, TextIndexHint.ALL));
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new ElasticSearchSearchGraphQuery(
                getClient(),
                graph,
                queryString,
                getAllPropertyDefinitions(),
                getConfig().getScoringStrategy(),
                getIndexSelectionStrategy(),
                authorizations);
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new ElasticSearchSearchVertexQuery(
                getClient(),
                graph,
                vertex,
                queryString,
                getAllPropertyDefinitions(),
                getConfig().getScoringStrategy(),
                getIndexSelectionStrategy(),
                authorizations);
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(Graph graph, String[] similarToFields, String similarToText, Authorizations authorizations) {
        return new ElasticSearchSearchGraphQuery(
                getClient(),
                graph,
                similarToFields, similarToText,
                getAllPropertyDefinitions(),
                getConfig().getScoringStrategy(),
                getIndexSelectionStrategy(),
                authorizations);
    }

    @Override
    public boolean isFieldLevelSecuritySupported() {
        return false;
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return SearchIndexSecurityGranularity.DOCUMENT;
    }

    @Override
    public Map<Object, Long> getVertexPropertyCountByValue(Graph graph, String propertyName, Authorizations authorizations) {
        PropertyDefinition propertyDefinition = getAllPropertyDefinitions().get(propertyName);
        if (propertyDefinition != null && propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
            propertyName = propertyDefinition.getPropertyName() + EXACT_MATCH_PROPERTY_NAME_SUFFIX;
        }

        String countAggName = "count";
        TermsBuilder countAgg = new TermsBuilder(countAggName)
                .field(propertyName)
                .size(500000);
        TermFilterBuilder elementTypeFilterBuilder = new TermFilterBuilder(ELEMENT_TYPE_FIELD_NAME, ElasticSearchElementType.VERTEX.getKey());
        FilteredQueryBuilder queryBuilder = QueryBuilders.filteredQuery(
                QueryBuilders.matchAllQuery(),
                elementTypeFilterBuilder
        );
        SearchRequestBuilder q = getClient().prepareSearch(getIndexNamesAsArray())
                .setQuery(queryBuilder)
                .setSearchType(SearchType.COUNT)
                .addAggregation(countAgg);
        if (ElasticSearchQueryBase.QUERY_LOGGER.isTraceEnabled()) {
            ElasticSearchQueryBase.QUERY_LOGGER.trace("query: %s", q);
        }
        SearchResponse response = getClient().search(q.request()).actionGet();
        Terms propertyCountResults = response.getAggregations().get(countAggName);

        Map<Object, Long> results = new HashMap<>();
        for (Terms.Bucket propertyCountResult : propertyCountResults.getBuckets()) {
            String key = propertyCountResult.getKey().toLowerCase();
            results.put(key, propertyCountResult.getDocCount());
        }
        return results;
    }
}
