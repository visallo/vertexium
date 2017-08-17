package org.vertexium.elasticsearch2;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.vertexium.*;
import org.vertexium.query.VertexQuery;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

public class ElasticsearchSearchVertexQuery extends ElasticsearchSearchQueryBase implements VertexQuery {
    private final Vertex sourceVertex;
    private Direction direction = Direction.BOTH;
    private String otherVertexId;

    public ElasticsearchSearchVertexQuery(
            Client client,
            Graph graph,
            Vertex sourceVertex,
            String queryString,
            Options options,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, options, authorizations);
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
        switch (direction) {
            case BOTH:
                QueryBuilder inVertexIdFilter = getDirectionInEdgeFilter();
                QueryBuilder outVertexIdFilter = getDirectionOutEdgeFilter();
                return QueryBuilders.boolQuery()
                        .should(inVertexIdFilter)
                        .should(outVertexIdFilter)
                        .minimumNumberShouldMatch(1);
            case OUT:
                return getDirectionOutEdgeFilter();
            case IN:
                return getDirectionInEdgeFilter();
            default:
                throw new VertexiumException("unexpected direction: " + direction);
        }
    }

    private QueryBuilder getDirectionInEdgeFilter() {
        QueryBuilder outVertexIdFilter = QueryBuilders.termQuery(Elasticsearch2SearchIndex.IN_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
        if (otherVertexId != null) {
            QueryBuilder inVertexIdFilter = QueryBuilders.termQuery(Elasticsearch2SearchIndex.OUT_VERTEX_ID_FIELD_NAME, otherVertexId);
            return QueryBuilders.boolQuery()
                    .must(outVertexIdFilter)
                    .must(inVertexIdFilter);
        }
        return outVertexIdFilter;
    }

    private QueryBuilder getDirectionOutEdgeFilter() {
        QueryBuilder outVertexIdFilter = QueryBuilders.termQuery(Elasticsearch2SearchIndex.OUT_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
        if (otherVertexId != null) {
            QueryBuilder inVertexIdFilter = QueryBuilders.termQuery(Elasticsearch2SearchIndex.IN_VERTEX_ID_FIELD_NAME, otherVertexId);
            return QueryBuilders.boolQuery()
                    .must(outVertexIdFilter)
                    .must(inVertexIdFilter);
        }
        return outVertexIdFilter;
    }

    private QueryBuilder getVertexFilter(EnumSet<ElasticsearchDocumentType> elementTypes) {
        List<QueryBuilder> filters = new ArrayList<>();
        List<String> edgeLabels = getParameters().getEdgeLabels();
        String[] edgeLabelsArray = edgeLabels == null || edgeLabels.size() == 0
                ? null
                : edgeLabels.toArray(new String[edgeLabels.size()]);
        Stream<EdgeInfo> edgeInfos = stream(sourceVertex.getEdgeInfos(
                direction,
                edgeLabelsArray,
                getParameters().getAuthorizations()
        ));
        if (otherVertexId != null) {
            edgeInfos = edgeInfos.filter(ei -> ei.getVertexId().equals(otherVertexId));
        }
        String[] ids = edgeInfos.map(EdgeInfo::getVertexId).toArray(String[]::new);

        if (elementTypes.contains(ElasticsearchDocumentType.VERTEX)) {
            filters.add(QueryBuilders.idsQuery().ids(ids));
        }

        if (elementTypes.contains(ElasticsearchDocumentType.VERTEX_EXTENDED_DATA)) {
            for (String vertexId : ids) {
                filters.add(
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery(Elasticsearch2SearchIndex.ELEMENT_TYPE_FIELD_NAME, ElasticsearchDocumentType.VERTEX_EXTENDED_DATA.getKey()))
                                .must(QueryBuilders.termQuery(Elasticsearch2SearchIndex.EXTENDED_DATA_ELEMENT_ID_FIELD_NAME, vertexId)));
            }
        }

        return orFilters(filters);
    }

    private QueryBuilder orFilters(List<QueryBuilder> filters) {
        if (filters.size() == 1) {
            return filters.get(0);
        } else {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            for (QueryBuilder filter : filters) {
                boolQuery.should(filter);
            }
            boolQuery.minimumNumberShouldMatch(1);
            return boolQuery;
        }
    }

    @Override
    public VertexQuery hasDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    @Override
    public VertexQuery hasOtherVertexId(String otherVertexId) {
        this.otherVertexId = otherVertexId;
        return this;
    }
}