package org.vertexium.elasticsearch;

import com.google.common.collect.Lists;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.vertexium.Authorizations;
import org.vertexium.Direction;
import org.vertexium.Graph;
import org.vertexium.Vertex;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.VertexQuery;

import java.util.ArrayList;
import java.util.EnumSet;
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
        FilterBuilder inVertexIdFilter = FilterBuilders.termFilter(ElasticsearchSingleDocumentSearchIndex.IN_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
        FilterBuilder outVertexIdFilter = FilterBuilders.termFilter(ElasticsearchSingleDocumentSearchIndex.OUT_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
        return FilterBuilders.orFilter(inVertexIdFilter, outVertexIdFilter);
    }

    private FilterBuilder getVertexFilter(EnumSet<ElasticsearchDocumentType> elementTypes) {
        List<FilterBuilder> filters = new ArrayList<>();
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
}
