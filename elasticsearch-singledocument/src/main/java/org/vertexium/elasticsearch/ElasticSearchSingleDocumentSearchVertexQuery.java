package org.vertexium.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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
            Authorizations authorizations
    ) {
        super(client, graph, queryString, scoringStrategy, indexSelectionStrategy, pageSize, authorizations);
        this.sourceVertex = sourceVertex;
    }

    @Override
    protected List<QueryBuilder> getFilters(ElasticSearchElementType elementType) {
        List<QueryBuilder> results = super.getFilters(elementType);
        if (elementType.equals(ElasticSearchElementType.VERTEX)) {
            String[] ids = toArray(sourceVertex.getVertexIds(Direction.BOTH, getParameters().getAuthorizations()), String.class);
            results.add(QueryBuilders.idsQuery().ids(ids));
        } else if (elementType.equals(ElasticSearchElementType.EDGE)) {
            QueryBuilder inVertexIdFilter = QueryBuilders.termQuery(ElasticsearchSingleDocumentSearchIndex.IN_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
            QueryBuilder outVertexIdFilter = QueryBuilders.termQuery(ElasticsearchSingleDocumentSearchIndex.OUT_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
            results.add(QueryBuilders.orQuery(inVertexIdFilter, outVertexIdFilter));
        }
        return results;
    }
}
