package org.vertexium.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.vertexium.*;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.VertexQuery;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

public class ElasticSearchSingleDocumentSearchVertexQuery extends ElasticSearchSingleDocumentSearchQueryBase implements VertexQuery {
    private final Vertex sourceVertex;
    private Direction direction = Direction.BOTH;
    private String otherVertexId;

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
    protected List<FilterBuilder> getFilters(EnumSet<ElasticsearchDocumentType> elementTypes) {
        List<FilterBuilder> filters = super.getFilters(elementTypes);

        List<FilterBuilder> relatedFilters = new ArrayList<>();

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

    private FilterBuilder getEdgeFilter() {
        switch (direction) {
            case BOTH:
                FilterBuilder inVertexIdFilter = getDirectionInEdgeFilter();
                FilterBuilder outVertexIdFilter = getDirectionOutEdgeFilter();
                return FilterBuilders.orFilter(inVertexIdFilter, outVertexIdFilter);
            case OUT:
                return getDirectionOutEdgeFilter();
            case IN:
                return getDirectionInEdgeFilter();
            default:
                throw new VertexiumException("unexpected direction: " + direction);
        }
    }

    private FilterBuilder getDirectionInEdgeFilter() {
        FilterBuilder outVertexIdFilter = FilterBuilders.termFilter(ElasticsearchSingleDocumentSearchIndex.IN_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
        if (otherVertexId != null) {
            FilterBuilder inVertexIdFilter = FilterBuilders.termFilter(ElasticsearchSingleDocumentSearchIndex.OUT_VERTEX_ID_FIELD_NAME, otherVertexId);
            return FilterBuilders.andFilter(outVertexIdFilter, inVertexIdFilter);
        }
        return outVertexIdFilter;
    }

    private FilterBuilder getDirectionOutEdgeFilter() {
        FilterBuilder outVertexIdFilter = FilterBuilders.termFilter(ElasticsearchSingleDocumentSearchIndex.OUT_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
        if (otherVertexId != null) {
            FilterBuilder inVertexIdFilter = FilterBuilders.termFilter(ElasticsearchSingleDocumentSearchIndex.IN_VERTEX_ID_FIELD_NAME, otherVertexId);
            return FilterBuilders.andFilter(outVertexIdFilter, inVertexIdFilter);
        }
        return outVertexIdFilter;
    }

    private FilterBuilder getVertexFilter(EnumSet<ElasticsearchDocumentType> elementTypes) {
        List<FilterBuilder> filters = new ArrayList<>();
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
            filters.add(FilterBuilders.idsFilter().ids(ids));
        }

        if (elementTypes.contains(ElasticsearchDocumentType.VERTEX_EXTENDED_DATA)) {
            for (String vertexId : ids) {
                filters.add(FilterBuilders.andFilter(
                        FilterBuilders.termFilter(ElasticsearchSingleDocumentSearchIndex.ELEMENT_TYPE_FIELD_NAME, ElasticsearchDocumentType.VERTEX_EXTENDED_DATA.getKey()),
                        FilterBuilders.termFilter(ElasticsearchSingleDocumentSearchIndex.EXTENDED_DATA_ELEMENT_ID_FIELD_NAME, vertexId)
                ));
            }
        }

        return orFilters(filters);
    }

    private FilterBuilder orFilters(List<FilterBuilder> filters) {
        if (filters.size() == 1) {
            return filters.get(0);
        } else {
            return FilterBuilders.orFilter(filters.toArray(new FilterBuilder[filters.size()]));
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
