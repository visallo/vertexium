package org.vertexium.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.vertexium.*;
import org.vertexium.elasticsearch.utils.GetResponseUtil;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchParentChildSearchIndex extends ElasticSearchSearchIndexBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticSearchParentChildSearchIndex.class);
    public static final String PROPERTY_TYPE = "property";
    private String[] parentDocumentFields;
    private NameSubstitutionStrategy nameSubstitutionStrategy;
    private final ThreadLocal<Map<IndexInfo, BulkRequest>> bulkRequestsByIndexInfo = new ThreadLocal<Map<IndexInfo, BulkRequest>>() {
        @Override
        protected Map<IndexInfo, BulkRequest> initialValue() {
            return new HashMap<>();
        }
    };

    public ElasticSearchParentChildSearchIndex(GraphConfiguration config) {
        super(config);
        this.nameSubstitutionStrategy = getConfig().getNameSubstitutionStrategy();
    }

    @Override
    protected void ensureMappingsCreated(IndexInfo indexInfo) {
        ParentChildIndexInfo parentChildIndexInfo = (ParentChildIndexInfo) indexInfo;
        super.ensureMappingsCreated(indexInfo);

        if (!parentChildIndexInfo.isPropertyTypeDefined()) {
            try {
                XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("_parent").field("type", ELEMENT_TYPE).endObject()
                        .startObject("_source").field("enabled", isStoreSourceData()).endObject()
                        .startObject("properties")
                        .startObject(VISIBILITY_FIELD_NAME)
                        .field("type", "string")
                        .field("analyzer", "keyword")
                        .field("index", "not_analyzed")
                        .field("store", "true")
                        .endObject();
                XContentBuilder mapping = mappingBuilder.endObject()
                        .endObject();

                getClient().admin().indices().preparePutMapping(indexInfo.getIndexName())
                        .setIgnoreConflicts(false)
                        .setType(PROPERTY_TYPE)
                        .setSource(mapping)
                        .execute()
                        .actionGet();
                parentChildIndexInfo.setPropertyTypeDefined(true);
            } catch (IOException e) {
                throw new VertexiumException("Could not add mappings to index: " + indexInfo.getIndexName(), e);
            }
        }
    }

    @Override
    protected IndexInfo createIndexInfo(String indexName) {
        return new ParentChildIndexInfo(indexName);
    }

    @Override
    protected void createIndexAddFieldsToElementType(XContentBuilder builder) throws IOException {
        super.createIndexAddFieldsToElementType(builder);
        builder
                .startObject(VISIBILITY_FIELD_NAME)
                .field("type", "string")
                .field("analyzer", "keyword")
                .field("index", "not_analyzed")
                .field("store", "true")
                .endObject();
    }

    @Override
    public void deleteElement(Graph graph, Element element, Authorizations authorizations) {
        String indexName = getIndexName(element);
        deleteChildDocuments(indexName, element);
        deleteParentDocument(indexName, element);
    }

    private void deleteChildDocuments(String indexName, Element element) {
        String parentId = element.getId();
        DeleteByQueryResponse response = getClient()
                .prepareDeleteByQuery(indexName)
                .setTypes(PROPERTY_TYPE)
                .setQuery(
                        QueryBuilders.termQuery("_parent", ELEMENT_TYPE + "#" + parentId)
                )
                .execute()
                .actionGet();
        if (response.status() != RestStatus.OK) {
            throw new VertexiumException("Could not delete child elements " + element.getId() + " (status: " + response.status() + ")");
        }
        if (LOGGER.isDebugEnabled()) {
            for (IndexDeleteByQueryResponse r : response) {
                LOGGER.debug("deleted child document %s", r.toString());
            }
        }
    }

    private void deleteParentDocument(String indexName, Element element) {
        String id = element.getId();
        LOGGER.debug("deleting parent document %s", id);
        DeleteResponse deleteResponse = getClient().delete(
                getClient()
                        .prepareDelete(indexName, ELEMENT_TYPE, id)
                        .request()
        ).actionGet();
        if (!deleteResponse.isFound()) {
            LOGGER.warn("Could not delete element %s", element.getId());
        }
    }

    @Override
    public void deleteProperty(
            Graph graph,
            Element element,
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility,
            Authorizations authorizations
    ) {
        propertyKey = this.nameSubstitutionStrategy.deflate(propertyKey);
        propertyName = this.nameSubstitutionStrategy.deflate(propertyName);
        String propertyString = propertyKey + ":" + propertyName + ":" + propertyVisibility.getVisibilityString();
        String indexName = getIndexName(element);
        String id = getChildDocId(element, propertyKey, propertyName, propertyVisibility);
        DeleteResponse deleteResponse = getClient().delete(
                getClient()
                        .prepareDelete(indexName, PROPERTY_TYPE, id)
                        .request()
        ).actionGet();
        if (!deleteResponse.isFound()) {
            LOGGER.warn("Could not delete property %s %s", element.getId(), propertyString);
        }
        LOGGER.debug("deleted property %s %s", element.getId(), propertyString);
    }

    @Override
    public void addElement(Graph graph, Element element, Authorizations authorizations) {
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("addElement: %s", element.getId());
        }
        if (!getConfig().isIndexEdges() && element instanceof Edge) {
            if (MUTATION_LOGGER.isDebugEnabled()) {
                MUTATION_LOGGER.debug("skipping edge: %s", element.getId());
            }
            return;
        }

        IndexInfo indexInfo = addPropertiesToIndex(element, element.getProperties());

        try {
            BulkRequest bulkRequest = getBulkRequest(indexInfo);
            addElementToBulkRequest(graph, bulkRequest, indexInfo, element, authorizations);
            if (getConfig().isAutoFlush()) {
                flush();
            }
        } catch (Exception e) {
            throw new VertexiumException("Could not add element", e);
        }

        getConfig().getScoringStrategy().addElement(this, graph, element, authorizations);
    }

    private BulkRequest getBulkRequest(IndexInfo indexInfo) {
        BulkRequest result = bulkRequestsByIndexInfo.get().get(indexInfo);
        if (result == null) {
            result = new BulkRequest();
            bulkRequestsByIndexInfo.get().put(indexInfo, result);
        }
        return result;
    }

    @Override
    public void flush() {
        try {
            for (Map.Entry<IndexInfo, BulkRequest> bulkRequest : this.bulkRequestsByIndexInfo.get().entrySet()) {
                if (bulkRequest.getValue().numberOfActions() > 0) {
                    doBulkRequest(bulkRequest.getValue());
                }
            }
        } finally {
            this.bulkRequestsByIndexInfo.get().clear();
        }
        super.flush();
    }

    @Override
    public void addElementToBulkRequest(Graph graph, BulkRequest bulkRequest, IndexInfo indexInfo, Element element, Authorizations authorizations) {
        try {
            IndexRequest parentDocumentIndexRequest = getParentDocumentIndexRequest(indexInfo, element, authorizations);
            if (parentDocumentIndexRequest != null) {
                bulkRequest.add(parentDocumentIndexRequest);
            }
            for (Property property : element.getProperties()) {
                IndexRequest propertyIndexRequest = getPropertyDocumentIndexRequest(indexInfo, element, property);
                if (propertyIndexRequest != null) {
                    bulkRequest.add(propertyIndexRequest);
                }
            }
        } catch (IOException ex) {
            throw new VertexiumException("Could not add element to bulk request", ex);
        }
    }

    @SuppressWarnings("unused")
    public IndexRequest getPropertyDocumentIndexRequest(Element element, Property property) throws IOException {
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName, isStoreSourceData());
        return getPropertyDocumentIndexRequest(indexInfo, element, property);
    }

    private IndexRequest getPropertyDocumentIndexRequest(IndexInfo indexInfo, Element element, Property property) throws IOException {
        XContentBuilder jsonBuilder = buildJsonContentFromProperty(indexInfo, property);
        if (jsonBuilder == null) {
            return null;
        }

        String id = getChildDocId(element, property);

        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("addElement child (%s): %s", element.getId(), jsonBuilder.string());
        }

        IndexRequestBuilder builder = getClient().prepareIndex(indexInfo.getIndexName(), PROPERTY_TYPE, id);
        builder = builder.setParent(element.getId());
        builder = builder.setSource(jsonBuilder);
        return builder.request();
    }

    private String getChildDocId(Element element, Property property) {
        return getChildDocId(element, property.getKey(), property.getName(), property.getVisibility());
    }

    private String getChildDocId(Element element, String key, String name, Visibility visibility) {
        return element.getId() + "_" + this.nameSubstitutionStrategy.deflate(name) + "_" + this.nameSubstitutionStrategy.deflate(key);
    }

    @SuppressWarnings("unused")
    public IndexRequest getParentDocumentIndexRequest(Element element, Authorizations authorizations) throws IOException {
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName, isStoreSourceData());
        return getParentDocumentIndexRequest(indexInfo, element, authorizations);
    }

    private IndexRequest getParentDocumentIndexRequest(IndexInfo indexInfo, Element element, Authorizations authorizations) throws IOException {
        boolean changed = false;
        XContentBuilder jsonBuilder;
        jsonBuilder = XContentFactory.jsonBuilder()
                .startObject();

        String id = element.getId();
        GetResponse existingParentDocument = getParentDocument(indexInfo, element.getId());
        if (existingParentDocument == null) {
            changed = true;
        }
        if (element instanceof Vertex) {
            jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, ELEMENT_TYPE_VERTEX);
            if (getConfig().getScoringStrategy().addFieldsToVertexDocument(this, jsonBuilder, (Vertex) element, existingParentDocument, authorizations)) {
                changed = true;
            }
        } else if (element instanceof Edge) {
            jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, ELEMENT_TYPE_EDGE);
            if (getConfig().getScoringStrategy().addFieldsToEdgeDocument(this, jsonBuilder, (Edge) element, existingParentDocument, authorizations)) {
                changed = true;
            }
        } else {
            throw new VertexiumException("Unexpected element type " + element.getClass().getName());
        }

        String visibilityString = element.getVisibility().getVisibilityString();
        jsonBuilder.field(VISIBILITY_FIELD_NAME, visibilityString);
        if (existingParentDocument == null || !visibilityString.equals(GetResponseUtil.getFieldValueString(existingParentDocument, VISIBILITY_FIELD_NAME))) {
            changed = true;
        }

        if (!changed) {
            return null;
        }
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("addElement parent: %s", jsonBuilder.string());
        }
        return new IndexRequest(indexInfo.getIndexName(), ELEMENT_TYPE, id).source(jsonBuilder);
    }

    private GetResponse getParentDocument(IndexInfo indexInfo, String elementId) {
        try {
            GetResponse response = getClient()
                    .prepareGet(indexInfo.getIndexName(), ELEMENT_TYPE, elementId)
                    .setFields(getParentDocumentFields())
                    .execute()
                    .get();
            if (!response.isExists()) {
                return null;
            }
            return response;
        } catch (Exception ex) {
            throw new VertexiumException("Could not get parent document: " + elementId, ex);
        }
    }

    private String[] getParentDocumentFields() {
        if (this.parentDocumentFields == null) {
            List<String> fields = new ArrayList<>();
            fields.add(ELEMENT_TYPE_FIELD_NAME);
            fields.add(VISIBILITY_FIELD_NAME);
            fields.addAll(getConfig().getScoringStrategy().getFieldNames());
            this.parentDocumentFields = fields.toArray(new String[fields.size()]);
        }
        return this.parentDocumentFields;
    }

    private XContentBuilder buildJsonContentFromProperty(IndexInfo indexInfo, Property property) throws IOException {
        XContentBuilder jsonBuilder;
        jsonBuilder = XContentFactory.jsonBuilder()
                .startObject();

        Object propertyValue = property.getValue();
        String propertyName = this.nameSubstitutionStrategy.deflate(property.getName());
        if (propertyValue != null && shouldIgnoreType(propertyValue.getClass())) {
            return null;
        } else if (propertyValue instanceof GeoPoint) {
            convertGeoPoint(jsonBuilder, property, (GeoPoint) propertyValue);
        } else if (propertyValue instanceof GeoCircle) {
            convertGeoCircle(jsonBuilder, property, (GeoCircle) propertyValue);
        } else if (propertyValue instanceof StreamingPropertyValue) {
            StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValue;
            if (!streamingPropertyValue.isSearchIndex()) {
                return null;
            }

            PropertyDefinition propertyDefinition = indexInfo.getPropertyDefinitions().get(propertyName);
            if (propertyDefinition != null && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                return null;
            }

            Class valueType = streamingPropertyValue.getValueType();
            if (valueType == String.class) {
                InputStream in = streamingPropertyValue.getInputStream();
                propertyValue = StreamUtils.toString(in);
                jsonBuilder.field(property.getName(), propertyValue);
            } else {
                throw new VertexiumException("Unhandled StreamingPropertyValue type: " + valueType.getName());
            }
        } else if (propertyValue instanceof String) {
            PropertyDefinition propertyDefinition = indexInfo.getPropertyDefinitions().get(propertyName);
            if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                jsonBuilder.field(propertyName + EXACT_MATCH_PROPERTY_NAME_SUFFIX, propertyValue);
            }
            if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                jsonBuilder.field(propertyName, propertyValue);
            }
        } else {
            if (propertyValue instanceof DateOnly) {
                propertyValue = ((DateOnly) propertyValue).getDate();
            }

            jsonBuilder.field(propertyName, propertyValue);
        }
        jsonBuilder.field(VISIBILITY_FIELD_NAME, property.getVisibility().getVisibilityString());

        return jsonBuilder;
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new ElasticSearchParentChildGraphQuery(
                getClient(),
                getConfig().getIndicesToQuery(),
                graph,
                queryString,
                getAllPropertyDefinitions(),
                getConfig().getScoringStrategy(),
                this.nameSubstitutionStrategy,
                authorizations);
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new ElasticSearchParentChildVertexQuery(
                getClient(),
                getConfig().getIndicesToQuery(),
                graph,
                vertex,
                queryString,
                getAllPropertyDefinitions(),
                getConfig().getScoringStrategy(),
                this.nameSubstitutionStrategy,
                authorizations);
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(Graph graph, String[] similarToFields, String similarToText, Authorizations authorizations) {
        return new ElasticSearchParentChildGraphQuery(
                getClient(),
                getConfig().getIndicesToQuery(),
                graph,
                similarToFields, similarToText,
                getAllPropertyDefinitions(),
                getConfig().getScoringStrategy(),
                this.nameSubstitutionStrategy,
                authorizations);
    }

    @Override
    public boolean isFieldLevelSecuritySupported() {
        return true;
    }

    @Override
    protected void addPropertyToIndex(IndexInfo indexInfo, String propertyName, Class dataType, boolean analyzed, Double boost) throws IOException {
        if (indexInfo.isPropertyDefined(propertyName)) {
            return;
        }

        if (shouldIgnoreType(dataType)) {
            return;
        }

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(PROPERTY_TYPE)
                .startObject("_parent").field("type", ELEMENT_TYPE).endObject()
                .startObject("properties")
                .startObject(propertyName)
                .field("store", isStoreSourceData());

        addTypeToMapping(mapping, propertyName, dataType, analyzed, boost);

        mapping
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        getClient()
                .admin()
                .indices()
                .preparePutMapping(indexInfo.getIndexName())
                .setIgnoreConflicts(false)
                .setType(PROPERTY_TYPE)
                .setSource(mapping)
                .execute()
                .actionGet();

        indexInfo.addPropertyDefinition(propertyName, new PropertyDefinition(propertyName, dataType, TextIndexHint.ALL));
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return SearchIndexSecurityGranularity.PROPERTY;
    }

    @Override
    public Map<Object, Long> getVertexPropertyCountByValue(String propertyName, Authorizations authorizations) {
        PropertyDefinition propertyDefinition = getAllPropertyDefinitions().get(propertyName);
        if (propertyDefinition != null && propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
            propertyName = propertyDefinition.getPropertyName() + EXACT_MATCH_PROPERTY_NAME_SUFFIX;
        }

        String countAggName = "count";
        TermsBuilder countAgg = new TermsBuilder(countAggName)
                .field(propertyName)
                .size(500000);
        AuthorizationFilterBuilder authorizationFilterBuilder = new AuthorizationFilterBuilder(authorizations.getAuthorizations());
        TermFilterBuilder elementTypeFilterBuilder = new TermFilterBuilder(ELEMENT_TYPE_FIELD_NAME, ELEMENT_TYPE_VERTEX);
        AndFilterBuilder andFilterBuilder = new AndFilterBuilder(authorizationFilterBuilder, elementTypeFilterBuilder);
        FilteredQueryBuilder elementQueryBuilder = QueryBuilders.filteredQuery(
                QueryBuilders.matchAllQuery(),
                andFilterBuilder
        );
        FilteredQueryBuilder query = QueryBuilders.filteredQuery(
                new HasParentQueryBuilder(ELEMENT_TYPE, elementQueryBuilder),
                new AuthorizationFilterBuilder(authorizations.getAuthorizations())
        );
        SearchRequestBuilder q = getClient().prepareSearch(getIndexNamesAsArray())
                .setQuery(query)
                .setSearchType(SearchType.COUNT)
                .addAggregation(countAgg);
        if (ElasticSearchQueryBase.QUERY_LOGGER.isTraceEnabled()) {
            ElasticSearchQueryBase.QUERY_LOGGER.trace("query: %s", q);
        }
        SearchResponse response = getClient().search(q.request()).actionGet();
        Terms propertyCountResults = response.getAggregations().get(countAggName);

        Map<Object, Long> results = new HashMap<>();
        for (Terms.Bucket propertyCountResult : propertyCountResults.getBuckets()) {
            results.put(propertyCountResult.getKey(), propertyCountResult.getDocCount());
        }
        return results;
    }
}
