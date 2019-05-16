package org.vertexium.elasticsearch5;

import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.vertexium.*;
import org.vertexium.elasticsearch5.utils.FlushObjectQueue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.vertexium.elasticsearch5.Elasticsearch5SearchIndex.*;

class AddElementService {
    private final Graph graph;
    private final Elasticsearch5SearchIndex searchIndex;
    private final FlushObjectQueue flushObjectQueue;
    private final IdStrategy idStrategy;
    private final IndexService indexService;
    private final PropertyNameService propertyNameService;

    public AddElementService(
        Graph graph,
        Elasticsearch5SearchIndex searchIndex,
        FlushObjectQueue flushObjectQueue,
        IdStrategy idStrategy,
        IndexService indexService,
        PropertyNameService propertyNameService
    ) {
        this.graph = graph;
        this.searchIndex = searchIndex;
        this.flushObjectQueue = flushObjectQueue;
        this.idStrategy = idStrategy;
        this.indexService = indexService;
        this.propertyNameService = propertyNameService;
    }

    public void addElement(Element element) {
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("addElement: %s", element.getId());
        }

        if (!searchIndex.getConfig().isIndexEdges() && element instanceof Edge) {
            return;
        }

        while (flushObjectQueue.containsElementId(element.getId())) {
            flushObjectQueue.flush();
        }

        UpdateRequestBuilder updateRequestBuilder = prepareUpdate(graph, element);
        searchIndex.addActionRequestBuilderForFlush(element, updateRequestBuilder);

        if (searchIndex.getConfig().isAutoFlush()) {
            searchIndex.flush(graph);
        }
    }

    private UpdateRequestBuilder prepareUpdate(Graph graph, Element element) {
        try {
            IndexInfo indexInfo = indexService.addPropertiesToIndex(graph, element, element.getProperties());
            XContentBuilder source = buildJsonContentFromElement(graph, element);
            if (MUTATION_LOGGER.isTraceEnabled()) {
                MUTATION_LOGGER.trace("addElement json: %s: %s", element.getId(), source.string());
            }

            indexService.pushChange(indexInfo.getIndexName());
            return getClient()
                .prepareUpdate(indexInfo.getIndexName(), getIdStrategy().getType(), getIdStrategy().createElementDocId(element))
                .setDocAsUpsert(true)
                .setDoc(source)
                .setRetryOnConflict(FlushObjectQueue.MAX_RETRIES);
        } catch (IOException e) {
            throw new VertexiumException("Could not add element", e);
        }
    }

    private Client getClient() {
        return searchIndex.getClient();
    }

    public XContentBuilder buildJsonContentFromElementLocation(ElementLocation elementLocation) {
        try {
            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()
                .startObject();

            String elementTypeVisibilityPropertyName = indexService.addElementTypeVisibilityPropertyToIndex(graph, elementLocation);

            jsonBuilder.field(ELEMENT_ID_FIELD_NAME, elementLocation.getId());
            jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, getElementTypeValueFromElementType(elementLocation.getElementType()));
            if (elementLocation.getElementType() == ElementType.VERTEX) {
                jsonBuilder.field(elementTypeVisibilityPropertyName, ElasticsearchDocumentType.VERTEX.getKey());
            } else if (elementLocation.getElementType() == ElementType.EDGE) {
                EdgeElementLocation edgeElementLocation = (EdgeElementLocation) elementLocation;
                jsonBuilder.field(elementTypeVisibilityPropertyName, ElasticsearchDocumentType.EDGE.getKey());
                jsonBuilder.field(IN_VERTEX_ID_FIELD_NAME, edgeElementLocation.getVertexId(Direction.IN));
                jsonBuilder.field(OUT_VERTEX_ID_FIELD_NAME, edgeElementLocation.getVertexId(Direction.OUT));
                jsonBuilder.field(EDGE_LABEL_FIELD_NAME, edgeElementLocation.getLabel());
            } else {
                throw new VertexiumException("Unexpected element type " + elementLocation.getElementType());
            }

            return jsonBuilder;
        } catch (IOException ex) {
            throw new VertexiumException("Could not build document", ex);
        }
    }

    private XContentBuilder buildJsonContentFromElement(Graph graph, Element element) throws IOException {
        searchIndex.ensureAdditionalVisibilitiesDefined(element.getAdditionalVisibilities());

        XContentBuilder jsonBuilder = buildJsonContentFromElementLocation(element);
        jsonBuilder.field(ADDITIONAL_VISIBILITY_FIELD_NAME, element.getAdditionalVisibilities());

        for (Visibility hiddenVisibility : element.getHiddenVisibilities()) {
            String hiddenVisibilityPropertyName = propertyNameService.addVisibilityToPropertyName(graph, HIDDEN_VERTEX_FIELD_NAME, hiddenVisibility);
            if (!indexService.isPropertyInIndex(graph, HIDDEN_VERTEX_FIELD_NAME, hiddenVisibility)) {
                String indexName = indexService.getIndexName(element);
                IndexInfo indexInfo = indexService.ensureIndexCreatedAndInitialized(indexName);
                indexService.addPropertyToIndex(graph, indexInfo, hiddenVisibilityPropertyName, hiddenVisibility, Boolean.class, false, false, false);
            }
            jsonBuilder.field(hiddenVisibilityPropertyName, true);
        }

        Map<String, Object> fields = indexService.getPropertiesAsFields(graph, element.getProperties());
        addFieldsMap(jsonBuilder, fields);

        jsonBuilder.endObject();
        return jsonBuilder;
    }

    public IdStrategy getIdStrategy() {
        return idStrategy;
    }

    private String getElementTypeValueFromElementType(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return ElasticsearchDocumentType.VERTEX.getKey();
            case EDGE:
                return ElasticsearchDocumentType.EDGE.getKey();
            default:
                throw new VertexiumException("Unhandled element type: " + elementType);
        }
    }

    private void addFieldsMap(XContentBuilder jsonBuilder, Map<String, Object> fields) throws IOException {
        for (Map.Entry<String, Object> property : fields.entrySet()) {
            String propertyKey = propertyNameService.replaceFieldnameDots(property.getKey());
            if (property.getValue() instanceof List) {
                List list = (List) property.getValue();
                jsonBuilder.field(propertyKey, list.toArray(new Object[0]));
            } else {
                jsonBuilder.field(propertyKey, property.getValue());
            }
        }
    }
}
