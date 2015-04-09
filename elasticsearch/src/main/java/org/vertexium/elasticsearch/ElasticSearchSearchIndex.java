package org.vertexium.elasticsearch;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.vertexium.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.query.GraphQuery;
import org.vertexium.query.SimilarToGraphQuery;
import org.vertexium.type.GeoPoint;
import org.vertexium.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertexium.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ElasticSearchSearchIndex extends ElasticSearchSearchIndexBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchSearchIndexBase.class);

    public ElasticSearchSearchIndex(GraphConfiguration config) {
        super(config);
    }

    @Override
    public void addElement(Graph graph, Element element, Authorizations authorizations) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("addElement: " + element.getId());
        }
        if (!getConfig().isIndexEdges() && element instanceof Edge) {
            return;
        }

        IndexInfo indexInfo = addPropertiesToIndex(element, element.getProperties());

        try {
            XContentBuilder jsonBuilder = buildJsonContentFromElement(graph, indexInfo, element, authorizations);

            IndexResponse response = getClient()
                    .prepareIndex(indexInfo.getIndexName(), ELEMENT_TYPE, element.getId())
                    .setSource(jsonBuilder.endObject())
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
            IndexRequest indexRequest = new IndexRequest(indexInfo.getIndexName(), ELEMENT_TYPE, element.getId()).source(json);
            bulkRequest.add(indexRequest);
        } catch (IOException ex) {
            throw new VertexiumException("Could not add element to bulk request", ex);
        }
    }

    @Override
    public void deleteElement(Graph graph, Element element, Authorizations authorizations) {
        String indexName = getIndexName(element);
        String id = element.getId();
        LOGGER.debug("deleting document " + id);
        DeleteResponse deleteResponse = getClient().delete(
                getClient()
                        .prepareDelete(indexName, ELEMENT_TYPE, id)
                        .request()
        ).actionGet();
        if (!deleteResponse.isFound()) {
            throw new VertexiumException("Could not delete element " + element.getId());
        }
    }

    public String createJsonForElement(Graph graph, Element element, Authorizations authorizations) {
        try {
            String indexName = getIndexName(element);
            IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName, getConfig().isStoreSourceData());
            return buildJsonContentFromElement(graph, indexInfo, element, authorizations).string();
        } catch (Exception e) {
            throw new VertexiumException("Could not create JSON for element", e);
        }
    }

    private XContentBuilder buildJsonContentFromElement(Graph graph, IndexInfo indexInfo, Element element, Authorizations authorizations) throws IOException {
        XContentBuilder jsonBuilder;
        jsonBuilder = XContentFactory.jsonBuilder()
                .startObject();

        element = requeryWithAuthsAndMergedElement(graph, element, authorizations);

        if (element instanceof Vertex) {
            jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, ELEMENT_TYPE_VERTEX);
            getConfig().getScoringStrategy().addFieldsToVertexDocument(this, jsonBuilder, (Vertex) element, null, authorizations);
        } else if (element instanceof Edge) {
            jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, ELEMENT_TYPE_EDGE);
            getConfig().getScoringStrategy().addFieldsToEdgeDocument(this, jsonBuilder, (Edge) element, null, authorizations);
        } else {
            throw new VertexiumException("Unexpected element type " + element.getClass().getName());
        }

        Set<String> visibilityStrings = new HashSet<>();
        visibilityStrings.add(element.getVisibility().getVisibilityString());

        for (Property property : element.getProperties()) {
            visibilityStrings.add(property.getVisibility().getVisibilityString());

            Object propertyValue = property.getValue();
            if (propertyValue != null && shouldIgnoreType(propertyValue.getClass())) {
                continue;
            } else if (propertyValue instanceof GeoPoint) {
                GeoPoint geoPoint = (GeoPoint) propertyValue;
                Map<String, Object> propertyValueMap = new HashMap<>();
                propertyValueMap.put("lat", geoPoint.getLatitude());
                propertyValueMap.put("lon", geoPoint.getLongitude());
                jsonBuilder.field(property.getName() + GEO_PROPERTY_NAME_SUFFIX, propertyValueMap);
                if (geoPoint.getDescription() != null) {
                    jsonBuilder.field(property.getName(), geoPoint.getDescription());
                }
                continue;
            } else if (propertyValue instanceof StreamingPropertyValue) {
                StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValue;
                if (!streamingPropertyValue.isSearchIndex()) {
                    continue;
                }

                PropertyDefinition propertyDefinition = indexInfo.getPropertyDefinitions().get(property.getName());
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
                PropertyDefinition propertyDefinition = indexInfo.getPropertyDefinitions().get(property.getName());
                if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                    jsonBuilder.field(property.getName() + EXACT_MATCH_PROPERTY_NAME_SUFFIX, propertyValue);
                }
                if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                    jsonBuilder.field(property.getName(), propertyValue);
                }
                continue;
            }

            if (propertyValue instanceof DateOnly) {
                propertyValue = ((DateOnly) propertyValue).getDate();
            }

            jsonBuilder.field(property.getName(), propertyValue);
        }

        String visibilityString = Visibility.and(visibilityStrings).getVisibilityString();
        jsonBuilder.field(VISIBILITY_FIELD_NAME, visibilityString);

        return jsonBuilder;
    }

    private Element requeryWithAuthsAndMergedElement(Graph graph, Element element, Authorizations authorizations) {
        Element existingElement;
        if (element instanceof Vertex) {
            existingElement = graph.getVertex(element.getId(), authorizations);
        } else if (element instanceof Edge) {
            existingElement = graph.getEdge(element.getId(), authorizations);
        } else {
            throw new VertexiumException("Unexpected element type " + element.getClass().getName());
        }
        if (existingElement == null) {
            return element;
        }

        LOGGER.debug("Reindexing element " + element.getId());
        existingElement.mergeProperties(element);

        return existingElement;
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
                .startObject(ELEMENT_TYPE)
                .startObject("properties")
                .startObject(propertyName);

        addTypeToMapping(mapping, propertyName, dataType, analyzed, boost);

        mapping
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        PutMappingResponse response = getClient()
                .admin()
                .indices()
                .preparePutMapping(indexInfo.getIndexName())
                .setIgnoreConflicts(false)
                .setType(ELEMENT_TYPE)
                .setSource(mapping)
                .execute()
                .actionGet();
        LOGGER.debug(response.toString());

        indexInfo.addPropertyDefinition(propertyName, new PropertyDefinition(propertyName, dataType, TextIndexHint.ALL));
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new ElasticSearchGraphQuery(
                getClient(),
                getConfig().getIndicesToQuery(),
                graph,
                queryString,
                getAllPropertyDefinitions(),
                getConfig().getScoringStrategy(),
                authorizations);
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(Graph graph, String[] similarToFields, String similarToText, Authorizations authorizations) {
        return new ElasticSearchGraphQuery(
                getClient(),
                getConfig().getIndicesToQuery(),
                graph,
                similarToFields, similarToText,
                getAllPropertyDefinitions(),
                getConfig().getScoringStrategy(),
                authorizations);
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return SearchIndexSecurityGranularity.DOCUMENT;
    }
}
