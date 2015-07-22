package org.vertexium.elasticsearch;

import com.google.common.hash.Hashing;
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
import org.vertexium.util.StreamUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticsearchSingleDocumentSearchIndex extends ElasticSearchSearchIndexBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticsearchSingleDocumentSearchIndex.class);
    private static final Charset UTF8 = Charset.forName("utf8");
    public static final Pattern PROPERTY_NAME_PATTERN = Pattern.compile("(.*?)_([0-9a-f]+)(_([a-z]+))?");
    public static final Pattern AGGREGATION_NAME_PATTERN = Pattern.compile("(.*?)_([0-9a-f]+)");
    private final NameSubstitutionStrategy nameSubstitutionStrategy;
    private Map<String, PropertyNameVisibilities> propertyNameVisibilitiesMap = new HashMap<>();

    public ElasticsearchSingleDocumentSearchIndex(GraphConfiguration config) {
        super(config);
        this.nameSubstitutionStrategy = getConfig().getNameSubstitutionStrategy();
    }

    @Override
    protected boolean isStoreSourceData() {
        if (!super.isStoreSourceData()) {
            LOGGER.warn("Storing source date is required for single document indexing, to prevent needing to re-query the vertex.");
        }
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
            XContentBuilder jsonBuilder = buildJsonContentFromElement(indexInfo, element, authorizations);
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

    private XContentBuilder buildJsonContentFromElement(IndexInfo indexInfo, Element element, Authorizations authorizations) throws IOException {
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

        Map<String, Object> properties = getProperties(element, indexInfo);
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

    private Map<String, Object> getProperties(Element element, IndexInfo indexInfo) throws IOException {
        Map<String, Object> propertiesMap = new HashMap<>();
        for (Property property : element.getProperties()) {
            addPropertyToMap(property, propertiesMap, indexInfo);
        }
        return propertiesMap;
    }

    private void addPropertyToMap(Property property, Map<String, Object> propertiesMap, IndexInfo indexInfo) throws IOException {
        Object propertyValue = property.getValue();
        String propertyName = deflatePropertyName(property);
        if (propertyValue != null && shouldIgnoreType(propertyValue.getClass())) {
            return;
        } else if (propertyValue instanceof GeoPoint) {
            convertGeoPoint(propertiesMap, property, (GeoPoint) propertyValue);
            return;
        } else if (propertyValue instanceof GeoCircle) {
            convertGeoCircle(propertiesMap, property, (GeoCircle) propertyValue);
            return;
        } else if (propertyValue instanceof StreamingPropertyValue) {
            StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValue;
            if (!streamingPropertyValue.isSearchIndex()) {
                return;
            }

            PropertyDefinition propertyDefinition = indexInfo.getPropertyDefinitions().get(propertyName);
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
            PropertyDefinition propertyDefinition = indexInfo.getPropertyDefinitions().get(propertyName);
            if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                addPropertyValueToPropertiesMap(propertiesMap, propertyName + EXACT_MATCH_PROPERTY_NAME_SUFFIX, propertyValue);
            }
            if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                addPropertyValueToPropertiesMap(propertiesMap, propertyName, propertyValue);
            }
            return;
        }

        if (propertyValue instanceof DateOnly) {
            propertyValue = ((DateOnly) propertyValue).getDate();
        }

        addPropertyValueToPropertiesMap(propertiesMap, propertyName, propertyValue);
    }

    @Override
    protected String deflatePropertyName(Property property) {
        String visibilitySuffix = getVisibilityHash(property.getName(), property.getVisibility());
        return this.nameSubstitutionStrategy.deflate(property.getName()) + visibilitySuffix;
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
            if (m.groupCount() >= 4 && m.group(4) != null) {
                string = m.group(1) + "_" + m.group(4);
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
            return m.group(2);
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
    public String[] getAllMatchingPropertyNames(String propertyName, Authorizations authorizations) {
        PropertyNameVisibilities propertyNameVisibilities = this.propertyNameVisibilitiesMap.get(propertyName);
        if (propertyNameVisibilities == null) {
            throw new VertexiumNoMatchingPropertiesException(propertyName);
        }
        Collection<String> suffixes = propertyNameVisibilities.getSuffixes(authorizations);
        if (suffixes.size() == 0) {
            throw new VertexiumNoMatchingPropertiesException(propertyName);
        }
        String[] results = new String[suffixes.size()];
        String deflatedPropertyName = this.nameSubstitutionStrategy.deflate(propertyName);
        int i = 0;
        for (String suffix : suffixes) {
            results[i++] = deflatedPropertyName + suffix;
        }
        return results;
    }

    private String getVisibilityHash(String propertyName, Visibility visibility) {
        PropertyNameVisibilities propertyNameVisibilities = this.propertyNameVisibilitiesMap.get(propertyName);
        if (propertyNameVisibilities == null) {
            propertyNameVisibilities = new PropertyNameVisibilities();
            this.propertyNameVisibilitiesMap.put(propertyName, propertyNameVisibilities);
        }
        return propertyNameVisibilities.getSuffix(visibility);
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
        PropertyDefinition propertyDefinition = getPropertyDefinition(graph, property.getName());
        if (propertyDefinition != null) {
            String deflatedPropertyName = deflatePropertyName(property);
            super.addPropertyDefinitionToIndex(graph, indexInfo, deflatedPropertyName, propertyDefinition);
        } else {
            super.addPropertyToIndex(graph, indexInfo, property);
        }

        propertyDefinition = getPropertyDefinition(graph, property.getName() + EXACT_MATCH_PROPERTY_NAME_SUFFIX);
        if (propertyDefinition != null) {
            String deflatedPropertyName = deflatePropertyName(property);
            super.addPropertyDefinitionToIndex(graph, indexInfo, deflatedPropertyName, propertyDefinition);
        }

        propertyDefinition = getPropertyDefinition(graph, property.getName() + GEO_PROPERTY_NAME_SUFFIX);
        if (propertyDefinition != null) {
            String deflatedPropertyName = deflatePropertyName(property);
            super.addPropertyDefinitionToIndex(graph, indexInfo, deflatedPropertyName, propertyDefinition);
        }
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

        PropertyDefinition propertyDefinition = new PropertyDefinition(propertyName, dataType, TextIndexHint.ALL);
        indexInfo.addPropertyDefinition(propertyName, propertyDefinition);
        savePropertyDefinition(graph, propertyName, propertyDefinition);
    }

    @Override
    public void addElementToBulkRequest(Graph graph, BulkRequest bulkRequest, IndexInfo indexInfo, Element element, Authorizations authorizations) {
        try {
            XContentBuilder json = buildJsonContentFromElement(indexInfo, element, authorizations);
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

        for (String p : getAllMatchingPropertyNames(propertyName, authorizations)) {
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
                Long previousValue = results.get(propertyCountResult.getKey());
                if (previousValue == null) {
                    previousValue = 0L;
                }
                results.put(propertyCountResult.getKey(), previousValue + propertyCountResult.getDocCount());
            }
        }
        return results;
    }

    private static class PropertyNameVisibilities {
        private Map<Visibility, String> visibilityToSuffixMap = new HashMap<>();

        public String getSuffix(Visibility visibility) {
            String suffix = visibilityToSuffixMap.get(visibility);
            if (suffix == null) {
                suffix = "_" + Hashing.murmur3_128().hashString(visibility.getVisibilityString(), UTF8).toString();
                visibilityToSuffixMap.put(visibility, suffix);
            }
            return suffix;
        }

        public Collection<String> getSuffixes(Authorizations authorizations) {
            List<String> suffixes = new ArrayList<>();
            for (Map.Entry<Visibility, String> e : visibilityToSuffixMap.entrySet()) {
                if (authorizations.canRead(e.getKey())) {
                    suffixes.add(e.getValue());
                }
            }
            return suffixes;
        }
    }
}
