package org.vertexium.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.vertexium.Authorizations;
import org.vertexium.Direction;
import org.vertexium.Graph;
import org.vertexium.Vertex;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.VertexQuery;

import java.util.List;

import static org.vertexium.util.IterableUtils.toArray;

public class ElasticSearchSingleDocumentSearchVertexQuery extends ElasticSearchSingleDocumentSearchQueryBase implements VertexQuery {
    private final Vertex sourceVertex;

    public ElasticSearchSingleDocumentSearchVertexQuery(
            Client client,
            Graph graph,
            Vertex sourceVertex,
            String queryString,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            int pageSize,
            int termAggregationShardSize,
            int termAggregationSize,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, scoringStrategy, indexSelectionStrategy, pageSize, termAggregationShardSize, termAggregationSize, authorizations);
        this.sourceVertex = sourceVertex;
    }

    @Override
    protected List<FilterBuilder> getFilters(ElasticSearchElementType elementType) {
        List<FilterBuilder> results = super.getFilters(elementType);
        if (elementType.equals(ElasticSearchElementType.VERTEX)) {
            List<String> edgeLabels = getParameters().getEdgeLabels();
            String[] edgeLabelsArray = edgeLabels == null || edgeLabels.size() == 0
                    ? null
                    : edgeLabels.toArray(new String[edgeLabels.size()]);
            Iterable<String> vertexIds = sourceVertex.getVertexIds(
                    Direction.BOTH,
                    edgeLabelsArray,
                    getParameters().getAuthorizations()
            );
            String[] ids = toArray(vertexIds, String.class);
            results.add(FilterBuilders.idsFilter().ids(ids));
        } else if (elementType.equals(ElasticSearchElementType.EDGE)) {
            FilterBuilder inVertexIdFilter = FilterBuilders.termFilter(ElasticsearchSingleDocumentSearchIndex.IN_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
            FilterBuilder outVertexIdFilter = FilterBuilders.termFilter(ElasticsearchSingleDocumentSearchIndex.OUT_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
            results.add(FilterBuilders.orFilter(inVertexIdFilter, outVertexIdFilter));
        }
        return results;
    }
}
