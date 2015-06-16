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
            String[] indicesToQuery,
            Graph graph,
            Vertex vertex,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            NameSubstitutionStrategy nameSubstitutionStrategy,
            Authorizations authorizations
    ) {
        super(client, indicesToQuery, graph, queryString, propertyDefinitions, scoringStrategy, nameSubstitutionStrategy, authorizations);
        this.sourceVertex = vertex;
    }

    @Override
    protected List<FilterBuilder> getElementFilters(String elementType) {
        List<FilterBuilder> results = super.getElementFilters(elementType);
        if (elementType.equals(ElasticSearchSearchIndexBase.ELEMENT_TYPE_VERTEX)) {
            String[] ids = toArray(sourceVertex.getVertexIds(Direction.BOTH, getParameters().getAuthorizations()), String.class);
            results.add(FilterBuilders.idsFilter().ids(ids));
        }
        return results;
    }
}
