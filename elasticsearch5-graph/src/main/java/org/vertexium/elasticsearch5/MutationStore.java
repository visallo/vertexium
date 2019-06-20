package org.vertexium.elasticsearch5;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.vertexium.*;
import org.vertexium.elasticsearch5.utils.SearchResponseUtils;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

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

    public Edge getEdges(String edgeId, FetchHints fetchHints, long endTime, User user) {
        return Elasticsearch5GraphEdge.createFromMutations(
            graph,
            edgeId,
            getMutations(ElementType.EDGE, edgeId, endTime),
            fetchHints,
            user,
            scriptService
        );
    }

    public Vertex getVertex(String vertexId, FetchHints fetchHints, long endTime, User user) {
        return Elasticsearch5GraphVertex.createFromMutations(
            graph,
            vertexId,
            getMutations(ElementType.VERTEX, vertexId, endTime),
            fetchHints,
            user,
            scriptService
        );
    }

    public Stream<Edge> getEdges(
        String outVertex,
        String inVertex,
        Direction direction,
        String[] labels,
        FetchHints fetchHints,
        long endTime,
        User user
    ) {
        Map<String, List<SearchHit>> mutationsByElementId = stream(getEdgeMutations(outVertex, inVertex, direction, labels, endTime))
            .collect(Collectors.groupingBy(m -> (String) m.getSource().get(FieldNames.MUTATION_ELEMENT_ID)));
        return mutationsByElementId.entrySet().stream()
            .map(entry -> {
                String elementId = entry.getKey();
                return Elasticsearch5GraphEdge.createFromMutations(
                    graph,
                    elementId,
                    entry.getValue(),
                    fetchHints,
                    user,
                    scriptService
                );
            });
    }

    private Iterable<SearchHit> getMutations(ElementType elementType, String elementId, long endTime) {
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery(FieldNames.MUTATION_ELEMENT_TYPE, elementType.name()))
            .must(QueryBuilders.termQuery(FieldNames.MUTATION_ELEMENT_ID, elementId))
            .must(QueryBuilders.rangeQuery(FieldNames.MUTATION_TIMESTAMP).lte(endTime));
        SearchResponse response = graph.getClient()
            .prepareSearch(indexSelectionStrategy.getMutationIndexNames(graph))
            .setScroll(new TimeValue(60000))
            .addSort(FieldNames.MUTATION_TIMESTAMP, SortOrder.ASC)
            .setQuery(query)
            .get();
        return SearchResponseUtils.scrollToIterable(graph.getClient(), response);
    }

    private Iterable<SearchHit> getEdgeMutations(
        String vertexId1,
        String vertexId2,
        Direction direction,
        String[] labels,
        long endTime
    ) {
        BoolQueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery(FieldNames.MUTATION_ELEMENT_TYPE, ElementType.EDGE.name()))
            .must(QueryBuilders.rangeQuery(FieldNames.MUTATION_TIMESTAMP).lte(endTime));
        if (labels != null) {
            query = query.must(QueryBuilders.termsQuery(FieldNames.MUTATION_EDGE_LABEL, labels));
        }
        switch (direction) {
            case OUT:
                if (vertexId1 != null) {
                    query = query.must(QueryBuilders.termsQuery(FieldNames.MUTATION_OUT_VERTEX_ID, vertexId1));
                }
                if (vertexId2 != null) {
                    query = query.must(QueryBuilders.termsQuery(FieldNames.MUTATION_IN_VERTEX_ID, vertexId2));
                }
                break;

            case IN:
                if (vertexId1 != null) {
                    query = query.must(QueryBuilders.termsQuery(FieldNames.MUTATION_IN_VERTEX_ID, vertexId1));
                }
                if (vertexId2 != null) {
                    query = query.must(QueryBuilders.termsQuery(FieldNames.MUTATION_OUT_VERTEX_ID, vertexId2));
                }
                break;

            case BOTH:
                BoolQueryBuilder v1to2 = QueryBuilders.boolQuery();
                if (vertexId1 != null) {
                    v1to2 = v1to2.must(QueryBuilders.termsQuery(FieldNames.MUTATION_OUT_VERTEX_ID, vertexId1));
                }
                if (vertexId2 != null) {
                    v1to2 = v1to2.must(QueryBuilders.termsQuery(FieldNames.MUTATION_IN_VERTEX_ID, vertexId2));
                }

                BoolQueryBuilder v2to1 = QueryBuilders.boolQuery();
                if (vertexId1 != null) {
                    v2to1 = v2to1.must(QueryBuilders.termsQuery(FieldNames.MUTATION_IN_VERTEX_ID, vertexId1));
                }
                if (vertexId2 != null) {
                    v2to1 = v2to1.must(QueryBuilders.termsQuery(FieldNames.MUTATION_OUT_VERTEX_ID, vertexId2));
                }

                BoolQueryBuilder q = QueryBuilders.boolQuery()
                    .should(v1to2)
                    .should(v2to1);
                query = query.must(q.minimumShouldMatch(1));
                break;
        }
        SearchResponse response = graph.getClient()
            .prepareSearch(indexSelectionStrategy.getMutationIndexNames(graph))
            .setScroll(new TimeValue(60000))
            .addSort(FieldNames.MUTATION_TIMESTAMP, SortOrder.ASC)
            .setQuery(query)
            .get();
        return SearchResponseUtils.scrollToIterable(graph.getClient(), response);
    }

    public Stream<HistoricalEvent> getHistoricalEvents(HistoricalEventId after, HistoricalEventsFetchHints fetchHints, User user) {
        throw new VertexiumException("not implemented");
    }
}
