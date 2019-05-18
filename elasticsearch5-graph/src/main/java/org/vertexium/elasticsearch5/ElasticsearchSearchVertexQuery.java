package org.vertexium.elasticsearch5;

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

public class ElasticsearchSearchVertexQuery extends ElasticsearchSearchQueryBase implements VertexQuery {
    private final Vertex sourceVertex;
    private Direction direction = Direction.BOTH;
    private String otherVertexId;

    public ElasticsearchSearchVertexQuery(
        Client client,
        Graph graph,
        IndexService indexService,
        PropertyNameService propertyNameService,
        PropertyNameVisibilitiesStore propertyNameVisibilitiesStore,
        IdStrategy idStrategy,
        String queryString,
        Vertex sourceVertex,
        Options options,
        Authorizations authorizations
    ) {
        super(
            client,
            graph,
            indexService,
            propertyNameService,
            propertyNameVisibilitiesStore,
            idStrategy,
            queryString,
            options,
            authorizations
        );
        this.sourceVertex = sourceVertex;
    }

    @Override
    protected List<QueryBuilder> getFilters(EnumSet<ElasticsearchDocumentType> elementTypes, FetchHints fetchHints) {
        List<QueryBuilder> filters = super.getFilters(elementTypes, fetchHints);

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
                    .minimumShouldMatch(1);
            case OUT:
                return getDirectionOutEdgeFilter();
            case IN:
                return getDirectionInEdgeFilter();
            default:
                throw new VertexiumException("unexpected direction: " + direction);
        }
    }

    private QueryBuilder getDirectionInEdgeFilter() {
        QueryBuilder outVertexIdFilter = QueryBuilders.termQuery(FieldNames.IN_VERTEX_ID, sourceVertex.getId());
        if (otherVertexId != null) {
            QueryBuilder inVertexIdFilter = QueryBuilders.termQuery(FieldNames.OUT_VERTEX_ID, otherVertexId);
            return QueryBuilders.boolQuery()
                .must(outVertexIdFilter)
                .must(inVertexIdFilter);
        }
        return outVertexIdFilter;
    }

    private QueryBuilder getDirectionOutEdgeFilter() {
        QueryBuilder outVertexIdFilter = QueryBuilders.termQuery(FieldNames.OUT_VERTEX_ID, sourceVertex.getId());
        if (otherVertexId != null) {
            QueryBuilder inVertexIdFilter = QueryBuilders.termQuery(FieldNames.IN_VERTEX_ID, otherVertexId);
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
            : edgeLabels.toArray(new String[0]);
        Stream<EdgeInfo> edgeInfos = sourceVertex.getEdgeInfos(
            direction,
            edgeLabelsArray,
            getParameters().getUser()
        );
        if (otherVertexId != null) {
            edgeInfos = edgeInfos.filter(ei -> ei.getVertexId().equals(otherVertexId));
        }
        if (getParameters().getIds() != null) {
            edgeInfos = edgeInfos.filter(ei -> getParameters().getIds().contains(ei.getVertexId()));
        }
        String[] ids = edgeInfos.map(EdgeInfo::getVertexId).toArray(String[]::new);

        if (elementTypes.contains(ElasticsearchDocumentType.VERTEX)) {
            filters.add(QueryBuilders.termsQuery(FieldNames.ELEMENT_ID, ids));
        }

        if (elementTypes.contains(ElasticsearchDocumentType.VERTEX_EXTENDED_DATA)) {
            for (String vertexId : ids) {
                filters.add(
                    QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(FieldNames.ELEMENT_TYPE, ElasticsearchDocumentType.VERTEX_EXTENDED_DATA.getKey()))
                        .must(QueryBuilders.termQuery(FieldNames.ELEMENT_ID, vertexId)));
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
            boolQuery.minimumShouldMatch(1);
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

    @Override
    public String toString() {
        return super.toString() +
            ", sourceVertex=" + sourceVertex +
            ", otherVertexId=" + otherVertexId +
            ", direction=" + direction;
    }
}