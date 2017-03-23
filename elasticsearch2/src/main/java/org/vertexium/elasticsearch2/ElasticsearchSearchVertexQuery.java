package org.vertexium.elasticsearch2;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.vertexium.Authorizations;
import org.vertexium.Direction;
import org.vertexium.Graph;
import org.vertexium.Vertex;
import org.vertexium.elasticsearch2.score.ScoringStrategy;
import org.vertexium.query.VertexQuery;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.vertexium.util.IterableUtils.toArray;

public class ElasticsearchSearchVertexQuery extends ElasticsearchSearchQueryBase implements VertexQuery {
    private final Vertex sourceVertex;

    public ElasticsearchSearchVertexQuery(
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
    protected List<QueryBuilder> getFilters(EnumSet<ElasticsearchDocumentType> elementTypes) {
        List<QueryBuilder> filters = super.getFilters(elementTypes);

        List<QueryBuilder> relatedFilters = new ArrayList<>();

        if (elementTypes.contains(ElasticsearchDocumentType.VERTEX)
                || elementTypes.contains(ElasticsearchDocumentType.VERTEX_EXTENDED_DATA)) {
            relatedFilters.add(getVertexFilter(elementTypes));
        }

        if (elementTypes.contains(ElasticsearchDocumentType.EDGE)
                || elementTypes.contains(ElasticsearchDocumentType.EDGE_EXTENDED_DATA)) {
            relatedFilters.add(getEdgeFilter());
        }

        filters.add(orFilters(relatedFilters));

        return filters;
    }

    private QueryBuilder getEdgeFilter() {
        QueryBuilder inVertexIdFilter = QueryBuilders.termQuery(Elasticsearch2SearchIndex.IN_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
        QueryBuilder outVertexIdFilter = QueryBuilders.termQuery(Elasticsearch2SearchIndex.OUT_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
        return QueryBuilders.orQuery(inVertexIdFilter, outVertexIdFilter);
    }

    private QueryBuilder getVertexFilter(EnumSet<ElasticsearchDocumentType> elementTypes) {
        List<QueryBuilder> filters = new ArrayList<>();
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

        if (elementTypes.contains(ElasticsearchDocumentType.VERTEX)) {
            filters.add(QueryBuilders.idsQuery().ids(ids));
        }

        if (elementTypes.contains(ElasticsearchDocumentType.VERTEX_EXTENDED_DATA)) {
            for (String vertexId : ids) {
                filters.add(QueryBuilders.andQuery(
                        QueryBuilders.termQuery(Elasticsearch2SearchIndex.ELEMENT_TYPE_FIELD_NAME, ElasticsearchDocumentType.VERTEX_EXTENDED_DATA.getKey()),
                        QueryBuilders.termQuery(Elasticsearch2SearchIndex.EXTENDED_DATA_ELEMENT_ID_FIELD_NAME, vertexId)
                ));
            }
        }

        return orFilters(filters);
    }

    private QueryBuilder orFilters(List<QueryBuilder> filters) {
        if (filters.size() == 1) {
            return filters.get(0);
        } else {
            return QueryBuilders.orQuery(filters.toArray(new QueryBuilder[filters.size()]));
        }
    }
}