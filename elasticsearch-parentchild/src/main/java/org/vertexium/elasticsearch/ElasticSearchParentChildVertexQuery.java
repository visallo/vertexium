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

public class ElasticSearchParentChildVertexQuery extends ElasticSearchParentChildQueryBase implements VertexQuery {
    private final Vertex sourceVertex;

    public ElasticSearchParentChildVertexQuery(
            TransportClient client,
            Graph graph,
            Vertex vertex,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            NameSubstitutionStrategy nameSubstitutionStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, propertyDefinitions, scoringStrategy, nameSubstitutionStrategy, indexSelectionStrategy, authorizations);
        this.sourceVertex = vertex;
    }

    @Override
    protected List<FilterBuilder> getElementFilters(ElasticSearchElementType elementType) {
        List<FilterBuilder> results = super.getElementFilters(elementType);
        if (elementType.equals(ElasticSearchElementType.VERTEX)) {
            String[] ids = toArray(sourceVertex.getVertexIds(Direction.BOTH, getParameters().getAuthorizations()), String.class);
            results.add(FilterBuilders.idsFilter().ids(ids));
        }
        return results;
    }
}
