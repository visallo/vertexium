package org.vertexium.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.vertexium.*;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.query.VertexQuery;

import java.util.List;
import java.util.Map;

import static org.vertexium.util.IterableUtils.toArray;

public class ElasticSearchSearchVertexQuery extends ElasticSearchSearchQueryBase implements VertexQuery {
    private final Vertex sourceVertex;

    public ElasticSearchSearchVertexQuery(
            TransportClient client,
            Graph graph,
            Vertex sourceVertex,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            NameSubstitutionStrategy nameSubstitutionStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, propertyDefinitions, scoringStrategy, nameSubstitutionStrategy, indexSelectionStrategy, authorizations);
        this.sourceVertex = sourceVertex;
    }

    @Override
    protected List<FilterBuilder> getFilters(ElasticSearchElementType elementType) {
        List<FilterBuilder> results = super.getFilters(elementType);
        if (elementType.equals(ElasticSearchElementType.VERTEX)) {
            String[] ids = toArray(sourceVertex.getVertexIds(Direction.BOTH, getParameters().getAuthorizations()), String.class);
            results.add(FilterBuilders.idsFilter().ids(ids));
        }
        return results;
    }
}

