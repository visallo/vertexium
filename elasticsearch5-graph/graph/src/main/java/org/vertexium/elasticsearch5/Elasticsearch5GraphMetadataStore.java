package org.vertexium.elasticsearch5;

import org.cache2k.Cache;
import org.cache2k.CacheBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.vertexium.GraphMetadataEntry;
import org.vertexium.GraphMetadataStore;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.utils.ProtobufUtils;
import org.vertexium.elasticsearch5.utils.SearchResponseUtils;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Elasticsearch5GraphMetadataStore extends GraphMetadataStore {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(Elasticsearch5GraphMetadataStore.class);
    private static final String VALID_AUTHORIZATIONS = "validAuthorizations";
    private static final String CACHE_KEY = "metadata";
    private final Elasticsearch5Graph graph;
    private final Client client;
    private final IndexService indexService;
    private final IndexSelectionStrategy indexSelectionStrategy;
    private final IdStrategy idStrategy;
    private final ScriptService scriptService;
    private final Cache<String, Map> metadataCache;

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
        this.metadataCache = CacheBuilder
            .newCache(String.class, Map.class)
            .name(Elasticsearch5GraphMetadataStore.class, "Elasticsearch5GraphMetadataStore-" + System.identityHashCode(this))
            .maxSize(1)
            .expiryDuration(60, TimeUnit.SECONDS)
            .source(unused -> {
                indexService.ensureMetadataIndexCreatedAndInitialized();
                QueryBuilder query = QueryBuilders.boolQuery()
                    .must(QueryBuilders.existsQuery(FieldNames.GRAPH_METADATA_NAME))
                    .must(QueryBuilders.existsQuery(FieldNames.GRAPH_METADATA_VALUE));
                LOGGER.debug("getMetadata()");
                SearchResponse response = client.prepareSearch(getIndexName())
                    .setScroll(new TimeValue(60000))
                    .setQuery(query)
                    .setSize(100)
                    .get();
                return SearchResponseUtils.scrollToStream(client, response)
                    .map(hit -> {
                        Map<String, Object> hitDoc = hit.getSourceAsMap();
                        String key = (String) hitDoc.get(FieldNames.GRAPH_METADATA_NAME);
                        Object value = scriptService.valueToJavaObject(ProtobufUtils.valueFromField(hitDoc.get(FieldNames.GRAPH_METADATA_VALUE)));
                        return new GraphMetadataEntry(key, value);
                    })
                    .collect(Collectors.toMap(GraphMetadataEntry::getKey, graphMetadataEntry -> graphMetadataEntry));
            })
            .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<GraphMetadataEntry> getMetadata() {
        return (Iterable<GraphMetadataEntry>) metadataCache.get(CACHE_KEY).values();
    }

    @Override
    public Object getMetadata(String key) {
        GraphMetadataEntry graphMetadataEntry = (GraphMetadataEntry) metadataCache.get(CACHE_KEY).get(key);
        if (graphMetadataEntry == null) {
            return null;
        }
        return graphMetadataEntry.getValue();
    }

    private String getIndexName() {
        return indexSelectionStrategy.getMetadataIndexName(graph);
    }

    @Override
    public void setMetadata(String key, Object value) {
        try {
            metadataCache.get(CACHE_KEY).put(key, new GraphMetadataEntry(key, value));

            indexService.ensureMetadataIndexCreatedAndInitialized();

            byte[] valueBinary = scriptService.objectToValue(value).toByteArray();
            XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .field(FieldNames.GRAPH_METADATA_NAME, key)
                .field(FieldNames.GRAPH_METADATA_VALUE, valueBinary)
                .endObject();
            LOGGER.debug("setMetadata(key=%s, value=%s)", key, value);
            client.prepareUpdate(getIndexName(), idStrategy.getMetadataType(), idStrategy.createMetadataDocId(key))
                .setDocAsUpsert(true)
                .setDoc(doc)
                .get();
            LOGGER.debug("refresh()");
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
