package org.vertexium.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.PropertyDefinition;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticSearchSingleDocumentSearchQueryBase extends ElasticSearchQueryBase implements
        GraphQueryWithHistogramAggregation,
        GraphQueryWithTermsAggregation,
        GraphQueryWithGeohashAggregation {
    private final List<HistogramQueryItem> histogramQueryItems = new ArrayList<>();
    private final List<TermsQueryItem> termsQueryItems = new ArrayList<>();
    private final List<GeohashQueryItem> geohashQueryItems = new ArrayList<>();

    public ElasticSearchSingleDocumentSearchQueryBase(
            TransportClient client,
            Graph graph,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, propertyDefinitions, scoringStrategy, indexSelectionStrategy, true, authorizations);
    }

    public ElasticSearchSingleDocumentSearchQueryBase(
            TransportClient client,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, similarToFields, similarToText, propertyDefinitions, scoringStrategy, indexSelectionStrategy, true, authorizations);
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
}

