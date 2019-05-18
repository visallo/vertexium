package org.vertexium.elasticsearch5;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.vertexium.*;
import org.vertexium.elasticsearch5.utils.SearchResponseUtils;

public class MutationStore {
    private final Elasticsearch5Graph graph;
    private final IndexSelectionStrategy indexSelectionStrategy;
    private final ScriptService scriptService;

    public MutationStore(
        Elasticsearch5Graph graph,
        IndexSelectionStrategy indexSelectionStrategy,
        ScriptService scriptService
    ) {
        this.graph = graph;
        this.indexSelectionStrategy = indexSelectionStrategy;
        this.scriptService = scriptService;
    }

    public Edge getEdge(String edgeId, FetchHints fetchHints, long endTime, User user) {
        SearchResponse results = getMutations(ElementType.EDGE, edgeId, endTime);
        return Elasticsearch5GraphEdge.createFromMutations(
            graph,
            edgeId,
            SearchResponseUtils.scrollToIterable(graph.getClient(), results),
            fetchHints,
            user,
            scriptService
        );
    }

    public Vertex getVertex(String vertexId, FetchHints fetchHints, long endTime, User user) {
        SearchResponse results = getMutations(ElementType.VERTEX, vertexId, endTime);
        return Elasticsearch5GraphVertex.createFromMutations(
            graph,
            vertexId,
            SearchResponseUtils.scrollToIterable(graph.getClient(), results),
            fetchHints,
            user,
            scriptService
        );
    }

    private SearchResponse getMutations(ElementType elementType, String elementId, long endTime) {
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery(FieldNames.MUTATION_ELEMENT_TYPE, elementType.name()))
            .must(QueryBuilders.termQuery(FieldNames.MUTATION_ELEMENT_ID, elementId))
            .must(QueryBuilders.rangeQuery(FieldNames.MUTATION_TIMESTAMP).lte(endTime));
        return graph.getClient()
            .prepareSearch(indexSelectionStrategy.getMutationIndexNames(graph))
            .setScroll(new TimeValue(60000))
            .addSort(FieldNames.MUTATION_TIMESTAMP, SortOrder.ASC)
            .setQuery(query)
            .get();
    }
}
