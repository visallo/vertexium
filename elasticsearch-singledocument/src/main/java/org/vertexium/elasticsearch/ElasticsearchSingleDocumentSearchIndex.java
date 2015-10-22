package org.vertexium.elasticsearch;

import com.google.common.base.Throwables;
import net.jodah.recurrent.Recurrent;
import net.jodah.recurrent.RetryPolicy;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
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
import org.vertexium.type.GeoShape;
import org.vertexium.util.ConfigurationUtils;
import org.vertexium.util.StreamUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import javax.xml.ws.Holder;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticsearchSingleDocumentSearchIndex extends ElasticSearchSearchIndexBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticsearchSingleDocumentSearchIndex.class);
    public static final Pattern PROPERTY_NAME_PATTERN = Pattern.compile("^(.*?)(_([0-9a-f]{32}))?(_([a-z]))?$");
    public static final Pattern AGGREGATION_NAME_PATTERN = Pattern.compile("(.*?)_([0-9a-f]+)");
    public static final String CONFIG_PROPERTY_NAME_VISIBILITIES_STORE = "propertyNameVisibilitiesStore";
    public static final Class<? extends PropertyNameVisibilitiesStore> DEFAULT_PROPERTY_NAME_VISIBILITIES_STORE = MetadataTablePropertyNameVisibilitiesStore.class;
    private final NameSubstitutionStrategy nameSubstitutionStrategy;
    private final PropertyNameVisibilitiesStore propertyNameVisibilitiesStore;
    private final Random random = new Random();

    public ElasticsearchSingleDocumentSearchIndex(Graph graph, GraphConfiguration config) {
        super(graph, config);
        this.nameSubstitutionStrategy = getConfig().getNameSubstitutionStrategy();
        this.propertyNameVisibilitiesStore = createPropertyNameVisibilitiesStore(graph, config);
    }

    private PropertyNameVisibilitiesStore createPropertyNameVisibilitiesStore(Graph graph, GraphConfiguration config) {
        String className = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_PROPERTY_NAME_VISIBILITIES_STORE, DEFAULT_PROPERTY_NAME_VISIBILITIES_STORE.getName());
        return ConfigurationUtils.createProvider(className, graph, config);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (propertyNameVisibilitiesStore instanceof Closeable) {
            try {
                ((Closeable) propertyNameVisibilitiesStore).close();
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }
    }

    @Override
    protected boolean isStoreSourceData() {
        return true;
    }

    @Override
    protected boolean getDefaultAllFieldEnabled() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void addElement(Graph graph, final Element element, Authorizations authorizations) {
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("addElement: %s", element.getId());
        }
        if (!getConfig().isIndexEdges() && element instanceof Edge) {
            return;
        }

        final IndexInfo indexInfo = addPropertiesToIndex(graph, element, element.getProperties());

        try {
            final XContentBuilder jsonBuilder = buildJsonContentFromElement(graph, element, authorizations);
            final XContentBuilder source = jsonBuilder.endObject();
            if (MUTATION_LOGGER.isTraceEnabled()) {
                MUTATION_LOGGER.trace("addElement json: %s: %s", element.getId(), source.string());
            }

            final Holder<Boolean> retrying = new Holder<>(false);

            Runnable update = new Runnable() {
                public void run() {
                    if (retrying.value) {
                        getClient().get(Requests.getRequest(indexInfo.getIndexName())
                                .id(element.getId())
                                .type(ELEMENT_TYPE)
                                .refresh(true))
                                .actionGet();
                    }

                    try {
                        UpdateResponse response = getClient()
                                .prepareUpdate(indexInfo.getIndexName(), ELEMENT_TYPE, element.getId())
                                .setDocAsUpsert(true)
                                .setDoc(source)
                                .execute()
                                .actionGet();
                        if (response.getId() == null) {
                            throw new VertexiumException("Could not index document " + element.getId());
                        }
                    } catch (VersionConflictEngineException e) {
                        LOGGER.warn("ES version conflict detected for id %s. Retrying.", element.getId());
                        retrying.value = true;
                        throw e;
                    }
                }
            };

            //noinspection unchecked
            RetryPolicy retryPolicy = new RetryPolicy()
                    .retryOn(VersionConflictEngineException.class)
                    .withDelay(100 + random.nextInt(50), TimeUnit.MILLISECONDS)
                    .withMaxRetries(3);

            Recurrent.run(update, retryPolicy);

            if (getConfig().isAutoFlush()) {
                flush();
            }
        } catch (Exception e) {
            throw new VertexiumException("Could not add element", e);
        }

        getConfig().getScoringStrategy().addElement(this, graph, element, authorizations);
    }

    private XContentBuilder buildJsonContentFromElement(Graph graph, Element element, Authorizations authorizations) throws IOException {
        XContentBuilder jsonBuilder;
        jsonBuilder = XContentFactory.jsonBuilder()
                .startObject();

        String elementTypeVisibilityPropertyName = addElementTypeVisibilityPropertyToIndex(graph, element);

        if (element instanceof Vertex) {
            jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, ElasticSearchElementType.VERTEX.getKey());
            jsonBuilder.field(elementTypeVisibilityPropertyName, ElasticSearchElementType.VERTEX.getKey());
            getConfig().getScoringStrategy().addFieldsToVertexDocument(this, jsonBuilder, (Vertex) element, null, authorizations);
        } else if (element instanceof Edge) {
            Edge edge = (Edge) element;
            jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, ElasticSearchElementType.EDGE.getKey());
            jsonBuilder.field(elementTypeVisibilityPropertyName, ElasticSearchElementType.VERTEX.getKey());
            getConfig().getScoringStrategy().addFieldsToEdgeDocument(this, jsonBuilder, edge, null, authorizations);
            jsonBuilder.field(IN_VERTEX_ID_FIELD_NAME, edge.getVertexId(Direction.IN));
            jsonBuilder.field(OUT_VERTEX_ID_FIELD_NAME, edge.getVertexId(Direction.OUT));
            jsonBuilder.field(EDGE_LABEL_FIELD_NAME, edge.getLabel());
        } else {
            throw new VertexiumException("Unexpected element type " + element.getClass().getName());
        }

        Map<String, Object> properties = getProperties(graph, element);
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

    private String addElementTypeVisibilityPropertyToIndex(Graph graph, Element element) throws IOException {
        String elementTypeVisibilityPropertyName = deflatePropertyName(graph, ELEMENT_TYPE_FIELD_NAME, element.getVisibility());
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName, isStoreSourceData());
        addPropertyToIndex(graph, indexInfo, elementTypeVisibilityPropertyName, String.class, false, false);
        return elementTypeVisibilityPropertyName;
    }

    private Map<String, Object> getProperties(Graph graph, Element element) throws IOException {
        Map<String, Object> propertiesMap = new HashMap<>();
        for (Property property : element.getProperties()) {
            addPropertyToMap(graph, property, propertiesMap);
        }
        return propertiesMap;
    }

    private void addPropertyToMap(Graph graph, Property property, Map<String, Object> propertiesMap) throws IOException {
        Object propertyValue = property.getValue();
        String propertyName = deflatePropertyName(graph, property);
        PropertyDefinition propertyDefinition = getPropertyDefinition(graph, propertyName);
        if (propertyValue != null && shouldIgnoreType(propertyValue.getClass())) {
            return;
        } else if (propertyValue instanceof GeoPoint) {
            convertGeoPoint(graph, propertiesMap, property, (GeoPoint) propertyValue);
            return;
        } else if (propertyValue instanceof GeoCircle) {
            convertGeoCircle(graph, propertiesMap, property, (GeoCircle) propertyValue);
            return;
        } else if (propertyValue instanceof StreamingPropertyValue) {
            StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValue;
            if (!streamingPropertyValue.isSearchIndex()) {
                return;
            }

            if (propertyDefinition != null && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                return;
            }

            Class valueType = streamingPropertyValue.getValueType();
            if (valueType == String.class) {
                InputStream in = streamingPropertyValue.getInputStream();
                propertyValue = StreamUtils.toString(in);
            } else {
                throw new VertexiumException("Unhandled StreamingPropertyValue type: " + valueType.getName());
            }
        } else if (propertyValue instanceof String) {
            if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                addPropertyValueToPropertiesMap(propertiesMap, propertyName + EXACT_MATCH_PROPERTY_NAME_SUFFIX, propertyValue);
            }
            if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                addPropertyValueToPropertiesMap(propertiesMap, propertyName, propertyValue);
            }
            if (propertyDefinition != null && propertyDefinition.isSortable()) {
                String s = ((String) propertyValue).substring(0, Math.min(100, ((String) propertyValue).length()));
                addPropertyValueToPropertiesMap(propertiesMap, propertyDefinition.getPropertyName(), s);
            }
            return;
        }

        if (propertyValue instanceof DateOnly) {
            propertyValue = ((DateOnly) propertyValue).getDate();
        }

        addPropertyValueToPropertiesMap(propertiesMap, propertyName, propertyValue);
        if (propertyDefinition != null && propertyDefinition.isSortable()) {
            addPropertyValueToPropertiesMap(propertiesMap, propertyDefinition.getPropertyName(), propertyValue);
        }
    }

    @Override
    protected String deflatePropertyName(Graph graph, Property property) {
        String propertyName = property.getName();
        Visibility propertyVisibility = property.getVisibility();
        return deflatePropertyName(graph, propertyName, propertyVisibility);
    }

    private String deflatePropertyName(Graph graph, String propertyName, Visibility propertyVisibility) {
        String visibilityHash = getVisibilityHash(graph, propertyName, propertyVisibility);
        return this.nameSubstitutionStrategy.deflate(propertyName) + "_" + visibilityHash;
    }

    @Override
    protected String inflatePropertyName(String string) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(string);
        if (m.matches()) {
            string = m.group(1);
        }
        return super.inflatePropertyName(string);
    }

    private String inflatePropertyNameWithTypeSuffix(String string) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(string);
        if (m.matches()) {
            if (m.groupCount() >= 5 && m.group(5) != null) {
                string = m.group(1) + "_" + m.group(5);
            } else {
                string = m.group(1);
            }
        }
        return super.inflatePropertyName(string);
    }

    @Override
    public String getPropertyVisibilityHashFromDeflatedPropertyName(String deflatedPropertyName) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(deflatedPropertyName);
        if (m.matches()) {
            return m.group(3);
        }
        throw new VertexiumException("Could not match property name: " + deflatedPropertyName);
    }

    @Override
    public String getAggregationName(String name) {
        Matcher m = AGGREGATION_NAME_PATTERN.matcher(name);
        if (m.matches()) {
            return m.group(1);
        }
        throw new VertexiumException("Could not get aggregation name from: " + name);
    }

    @Override
    public String[] getAllMatchingPropertyNames(Graph graph, String propertyName, Authorizations authorizations) {
        Collection<String> hashes = this.propertyNameVisibilitiesStore.getHashes(graph, propertyName, authorizations);
        if (hashes.size() == 0) {
            return new String[0];
        }
        String[] results = new String[hashes.size()];
        String deflatedPropertyName = this.nameSubstitutionStrategy.deflate(propertyName);
        int i = 0;
        for (String hash : hashes) {
            results[i++] = deflatedPropertyName + "_" + hash;
        }
        return results;
    }

    public Collection<String> getQueryablePropertyNames(Graph graph, boolean includeNonStringFields, Authorizations authorizations) {
        Set<String> propertyNames = new HashSet<>();
        for (PropertyDefinition propertyDefinition : getAllPropertyDefinitions().values()) {
            List<String> queryableTypeSuffixes = getQueryableTypeSuffixes(propertyDefinition, includeNonStringFields);
            if (queryableTypeSuffixes.size() == 0) {
                continue;
            }
            String inflatedPropertyName = inflatePropertyName(propertyDefinition.getPropertyName()); // could be stored deflated
            String deflatedPropertyName = this.nameSubstitutionStrategy.deflate(inflatedPropertyName);
            if (isReservedFieldName(inflatedPropertyName)) {
                continue;
            }
            for (String hash : this.propertyNameVisibilitiesStore.getHashes(graph, inflatedPropertyName, authorizations)) {
                for (String typeSuffix : queryableTypeSuffixes) {
                    propertyNames.add(deflatedPropertyName + "_" + hash + typeSuffix);
                }
            }
        }
        return propertyNames;
    }

    public Collection<String> getQueryableElementTypeVisibilityPropertyNames(Graph graph, Authorizations authorizations) {
        Set<String> propertyNames = new HashSet<>();
        for (PropertyDefinition propertyDefinition : getAllPropertyDefinitions().values()) {
            String inflatedPropertyName = inflatePropertyName(propertyDefinition.getPropertyName()); // could be stored deflated
            String deflatedPropertyName = this.nameSubstitutionStrategy.deflate(inflatedPropertyName);
            if (!inflatedPropertyName.equals(ELEMENT_TYPE_FIELD_NAME)) {
                continue;
            }
            for (String hash : this.propertyNameVisibilitiesStore.getHashes(graph, inflatedPropertyName, authorizations)) {
                propertyNames.add(deflatedPropertyName + "_" + hash);
            }
        }
        if (propertyNames.size() == 0) {
            throw new VertexiumNoMatchingPropertiesException("No queryable " + ELEMENT_TYPE_FIELD_NAME + " for authorizations " + authorizations);
        }
        return propertyNames;
    }

    private List<String> getQueryableTypeSuffixes(PropertyDefinition propertyDefinition, boolean includeNonStringFields) {
        List<String> typeSuffixes = new ArrayList<>();
        if (propertyDefinition.getDataType() == String.class) {
            if (propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                typeSuffixes.add(EXACT_MATCH_PROPERTY_NAME_SUFFIX);
            }
            if (propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                typeSuffixes.add("");
            }
        } else if (propertyDefinition.getDataType() == GeoPoint.class
                || propertyDefinition.getDataType() == GeoCircle.class) {
            typeSuffixes.add("");
        } else if (includeNonStringFields && !shouldIgnoreType(propertyDefinition.getDataType())) {
            typeSuffixes.add("");
        }
        return typeSuffixes;
    }

    private String getVisibilityHash(Graph graph, String propertyName, Visibility visibility) {
        return this.propertyNameVisibilitiesStore.getHash(graph, propertyName, visibility);
    }

    @Override
    public void deleteElement(Graph graph, Element element, Authorizations authorizations) {
        String indexName = getIndexName(element);
        String id = element.getId();
        if (MUTATION_LOGGER.isTraceEnabled()) {
            LOGGER.trace("deleting document %s", id);
        }
        getClient().delete(
                getClient()
                        .prepareDelete(indexName, ELEMENT_TYPE, id)
                        .request()
        ).actionGet();
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return SearchIndexSecurityGranularity.PROPERTY;
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new ElasticSearchSingleDocumentSearchGraphQuery(
                getClient(),
                graph,
                queryString,
                getAllPropertyDefinitions(),
                getConfig().getScoringStrategy(),
                getIndexSelectionStrategy(),
                getConfig().getQueryPageSize(),
                authorizations);
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new ElasticSearchSingleDocumentSearchVertexQuery(
                getClient(),
                graph,
                vertex,
                queryString,
                getAllPropertyDefinitions(),
                getConfig().getScoringStrategy(),
                getIndexSelectionStrategy(),
                getConfig().getQueryPageSize(),
                authorizations);
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(Graph graph, String[] similarToFields, String similarToText, Authorizations authorizations) {
        return new ElasticSearchSingleDocumentSearchGraphQuery(
                getClient(),
                graph,
                similarToFields,
                similarToText,
                getAllPropertyDefinitions(),
                getConfig().getScoringStrategy(),
                getIndexSelectionStrategy(),
                getConfig().getQueryPageSize(),
                authorizations);
    }

    @Override
    public boolean isFieldLevelSecuritySupported() {
        return true;
    }

    @Override
    protected void addPropertyDefinitionToIndex(Graph graph, IndexInfo indexInfo, String propertyName, PropertyDefinition propertyDefinition) throws IOException {
        // unlike our super class we need to lazily add property definitions to the index because the property names are different depending on visibility.
    }

    @Override
    public PropertyDefinition getPropertyDefinition(Graph graph, String propertyName) {
        propertyName = inflatePropertyNameWithTypeSuffix(propertyName);
        return ((GraphBaseWithSearchIndex) graph).getPropertyDefinition(propertyName);
    }

    private void savePropertyDefinition(Graph graph, String propertyName, PropertyDefinition propertyDefinition) {
        propertyName = inflatePropertyNameWithTypeSuffix(propertyName);
        ((GraphBaseWithSearchIndex) graph).savePropertyDefinition(propertyName, propertyDefinition);
    }

    @Override
    public void addPropertyToIndex(Graph graph, IndexInfo indexInfo, Property property) throws IOException {
        // unlike the super class we need to lookup property definitions based on the property name without
        // the hash and define the property that way.
        Object propertyValue = property.getValue();

        PropertyDefinition propertyDefinition = getPropertyDefinition(graph, property.getName());
        if (propertyDefinition != null) {
            String deflatedPropertyName = deflatePropertyName(graph, property);
            super.addPropertyDefinitionToIndex(graph, indexInfo, deflatedPropertyName, propertyDefinition);
        } else {
            super.addPropertyToIndex(graph, indexInfo, property);
        }

        propertyDefinition = getPropertyDefinition(graph, property.getName() + EXACT_MATCH_PROPERTY_NAME_SUFFIX);
        if (propertyDefinition != null) {
            String deflatedPropertyName = deflatePropertyName(graph, property);
            super.addPropertyDefinitionToIndex(graph, indexInfo, deflatedPropertyName, propertyDefinition);
        }

        if (propertyValue instanceof GeoShape) {
            propertyDefinition = getPropertyDefinition(graph, property.getName() + GEO_PROPERTY_NAME_SUFFIX);
            if (propertyDefinition != null) {
                String deflatedPropertyName = deflatePropertyName(graph, property);
                super.addPropertyDefinitionToIndex(graph, indexInfo, deflatedPropertyName, propertyDefinition);
            }
        }
    }

    @Override
    protected void addPropertyToIndex(Graph graph, IndexInfo indexInfo, String propertyName, Class dataType, boolean analyzed, Double boost, boolean sortable) throws IOException {
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

        PropertyDefinition propertyDefinition = getPropertyDefinition(graph, inflatePropertyName(propertyName));
        if (propertyDefinition == null) {
            propertyDefinition = new PropertyDefinition(propertyName, dataType, TextIndexHint.ALL);
        }
        indexInfo.addPropertyDefinition(propertyName, propertyDefinition);
        savePropertyDefinition(graph, propertyName, propertyDefinition);
    }

    @Override
    public void addElementToBulkRequest(Graph graph, BulkRequest bulkRequest, IndexInfo indexInfo, Element element, Authorizations authorizations) {
        try {
            XContentBuilder json = buildJsonContentFromElement(graph, element, authorizations);
            UpdateRequest indexRequest = new UpdateRequest(indexInfo.getIndexName(), ELEMENT_TYPE, element.getId()).doc(json);
            indexRequest.docAsUpsert(true);
            bulkRequest.add(indexRequest);
        } catch (IOException ex) {
            throw new VertexiumException("Could not add element to bulk request", ex);
        }
    }

    @Override
    public Map<Object, Long> getVertexPropertyCountByValue(Graph graph, String propertyName, Authorizations authorizations) {
        TermFilterBuilder elementTypeFilterBuilder = new TermFilterBuilder(ELEMENT_TYPE_FIELD_NAME, ElasticSearchElementType.VERTEX.getKey());
        FilteredQueryBuilder queryBuilder = QueryBuilders.filteredQuery(
                QueryBuilders.matchAllQuery(),
                elementTypeFilterBuilder
        );
        SearchRequestBuilder q = getClient().prepareSearch(getIndexNamesAsArray())
                .setQuery(queryBuilder)
                .setSearchType(SearchType.COUNT);

        for (String p : getAllMatchingPropertyNames(graph, propertyName, authorizations)) {
            String countAggName = "count-" + p;
            PropertyDefinition propertyDefinition = getPropertyDefinition(graph, p);
            if (propertyDefinition != null && propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                p = p + EXACT_MATCH_PROPERTY_NAME_SUFFIX;
            }

            TermsBuilder countAgg = new TermsBuilder(countAggName)
                    .field(p)
                    .size(500000);
            q = q.addAggregation(countAgg);
        }

        if (ElasticSearchQueryBase.QUERY_LOGGER.isTraceEnabled()) {
            ElasticSearchQueryBase.QUERY_LOGGER.trace("query: %s", q);
        }
        SearchResponse response = getClient().search(q.request()).actionGet();
        Map<Object, Long> results = new HashMap<>();
        for (Aggregation agg : response.getAggregations().asList()) {
            Terms propertyCountResults = (Terms) agg;
            for (Terms.Bucket propertyCountResult : propertyCountResults.getBuckets()) {
                String mapKey = propertyCountResult.getKey().toLowerCase();
                Long previousValue = results.get(mapKey);
                if (previousValue == null) {
                    previousValue = 0L;
                }
                results.put(mapKey, previousValue + propertyCountResult.getDocCount());
            }
        }
        return results;
    }
}
