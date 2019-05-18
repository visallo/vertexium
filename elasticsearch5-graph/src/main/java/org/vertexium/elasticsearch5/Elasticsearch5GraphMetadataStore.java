package org.vertexium.elasticsearch5;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.vertexium.GraphMetadataEntry;
import org.vertexium.GraphMetadataStore;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.utils.ProtobufUtils;
import org.vertexium.util.IterableUtils;

import java.io.IOException;
import java.util.*;

public class Elasticsearch5GraphMetadataStore extends GraphMetadataStore {
    private static final String VALID_AUTHORIZATIONS = "validAuthorizations";
    private final Elasticsearch5Graph graph;
    private final Client client;
    private final IndexService indexService;
    private final IndexSelectionStrategy indexSelectionStrategy;
    private final IdStrategy idStrategy;
    private final ScriptService scriptService;

    public Elasticsearch5GraphMetadataStore(
        Elasticsearch5Graph graph,
        Client client,
        IndexService indexService,
        IndexSelectionStrategy indexSelectionStrategy,
        IdStrategy idStrategy,
        ScriptService scriptService
    ) {
        this.graph = graph;
        this.client = client;
        this.indexService = indexService;
        this.indexSelectionStrategy = indexSelectionStrategy;
        this.idStrategy = idStrategy;
        this.scriptService = scriptService;
    }

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        indexService.ensureMetadataIndexCreatedAndInitialized();
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.existsQuery(FieldNames.GRAPH_METADATA_NAME))
            .must(QueryBuilders.existsQuery(FieldNames.GRAPH_METADATA_VALUE));
        SearchResponse response = client.prepareSearch(getIndexName())
            .setScroll(new TimeValue(60000))
            .setQuery(query)
            .setSize(100)
            .get();
        List<GraphMetadataEntry> results = new ArrayList<>();
        do {
            for (SearchHit hit : response.getHits().getHits()) {
                Map<String, Object> hitDoc = hit.getSourceAsMap();
                String key = (String) hitDoc.get(FieldNames.GRAPH_METADATA_NAME);
                Object value = scriptService.valueToJavaObject(ProtobufUtils.valueFromField(hitDoc.get(FieldNames.GRAPH_METADATA_VALUE)));
                results.add(new GraphMetadataEntry(key, value));
            }

            response = client.prepareSearchScroll(response.getScrollId())
                .setScroll(new TimeValue(60000))
                .execute()
                .actionGet();
        } while (response.getHits().getHits().length != 0);
        return results;
    }

    @Override
    public Object getMetadata(String key) {
        indexService.ensureMetadataIndexCreatedAndInitialized();
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery(FieldNames.GRAPH_METADATA_NAME, key))
            .must(QueryBuilders.existsQuery(FieldNames.GRAPH_METADATA_VALUE));
        SearchHits hits = client.prepareSearch(getIndexName())
            .setQuery(query)
            .setSize(2)
            .get()
            .getHits();
        if (hits.getTotalHits() == 0) {
            return null;
        }
        if (hits.getTotalHits() > 1) {
            throw new VertexiumException("Found multiple metadata entries for key: " + key);
        }
        SearchHit hit = hits.getHits()[0];
        Map<String, Object> hitDoc = hit.getSourceAsMap();
        return scriptService.valueToJavaObject(ProtobufUtils.valueFromField(hitDoc.get(FieldNames.GRAPH_METADATA_VALUE)));
    }

    private String getIndexName() {
        return indexSelectionStrategy.getMetadataIndexName(graph);
    }

    @Override
    public void setMetadata(String key, Object value) {
        try {
            indexService.ensureMetadataIndexCreatedAndInitialized();

            byte[] valueBinary = scriptService.objectToValue(value).toByteArray();
            XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .field(FieldNames.GRAPH_METADATA_NAME, key)
                .field(FieldNames.GRAPH_METADATA_VALUE, valueBinary)
                .endObject();
            client.prepareUpdate(getIndexName(), idStrategy.getMetadataType(), idStrategy.createMetadataDocId(key))
                .setDocAsUpsert(true)
                .setDoc(doc)
                .get();
            client.admin().indices().prepareRefresh(getIndexName()).get();
        } catch (IOException ex) {
            throw new VertexiumException("Failed to set metadata", ex);
        }
    }

    public synchronized void addValidAuthorization(String authorization) {
        Set<String> validAuthorizations = getValidAuthorizations();
        validAuthorizations.add(authorization);
        setMetadata(VALID_AUTHORIZATIONS, validAuthorizations);
    }

    @SuppressWarnings("unchecked")
    public synchronized Set<String> getValidAuthorizations() {
        Object value = getMetadata(VALID_AUTHORIZATIONS);
        if (value == null) {
            return new HashSet<>();
        }
        return (Set<String>) IterableUtils.toSet((Iterable) value);
    }
}
