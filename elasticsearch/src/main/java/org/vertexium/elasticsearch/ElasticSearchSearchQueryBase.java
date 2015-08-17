package org.vertexium.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.vertexium.*;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticSearchSearchQueryBase extends ElasticSearchQueryBase implements
        GraphQueryWithHistogramAggregation,
        GraphQueryWithTermsAggregation,
        GraphQueryWithGeohashAggregation {
    private final List<HistogramQueryItem> histogramQueryItems = new ArrayList<>();
    private final List<TermsQueryItem> termsQueryItems = new ArrayList<>();
    private final List<GeohashQueryItem> geohashQueryItems = new ArrayList<>();

    public ElasticSearchSearchQueryBase(
            TransportClient client,
            Graph graph,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, propertyDefinitions, scoringStrategy, indexSelectionStrategy, true, true, false, authorizations);
    }

    public ElasticSearchSearchQueryBase(
            TransportClient client,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, similarToFields, similarToText, propertyDefinitions, scoringStrategy, indexSelectionStrategy, true, true, false, authorizations);
    }

    @Override
    public GraphQueryWithHistogramAggregation addHistogramAggregation(String aggregationName, String fieldName, String interval) {
        histogramQueryItems.add(new HistogramQueryItem(aggregationName, fieldName, interval));
        return this;
    }

    @Override
    public GraphQueryWithTermsAggregation addTermsAggregation(String aggregationName, String fieldName) {
        termsQueryItems.add(new TermsQueryItem(aggregationName, fieldName));
        return this;
    }

    @Override
    public GraphQueryWithGeohashAggregation addGeohashAggregation(String aggregationName, String fieldName, int precision) {
        geohashQueryItems.add(new GeohashQueryItem(aggregationName, fieldName, precision));
        return this;
    }

    @Override
    protected SearchRequestBuilder getSearchRequestBuilder(List<FilterBuilder> filters, QueryBuilder queryBuilder, ElasticSearchElementType elementType) {
        SearchRequestBuilder searchRequestBuilder = super.getSearchRequestBuilder(filters, queryBuilder, elementType);
        addHistogramQueryToSearchRequestBuilder(searchRequestBuilder, histogramQueryItems);
        addTermsQueryToSearchRequestBuilder(searchRequestBuilder, termsQueryItems);
        addGeohashQueryToSearchRequestBuilder(searchRequestBuilder, geohashQueryItems);
        return searchRequestBuilder;
    }

    @Override
    protected void applySort(SearchRequestBuilder q) {
        for (SortContainer sortContainer : getParameters().getSortContainers()) {
            SortOrder esOrder = sortContainer.direction == SortDirection.ASCENDING ? SortOrder.ASC : SortOrder.DESC;
            PropertyDefinition propertyDefinition = getSearchIndex().getPropertyDefinition(getGraph(), sortContainer.propertyName);
            if (propertyDefinition == null) {
                throw new VertexiumException("Could not find property definition for field: " + sortContainer.propertyName);
            }
            if (!propertyDefinition.isSortable()) {
                throw new VertexiumException("Cannot sort on non-sortable fields");
            }
            String suffix = propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH) ? ElasticSearchSearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX : "";
            q.addSort(propertyDefinition.getPropertyName() + suffix, esOrder);
        }
    }
}

