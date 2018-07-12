package org.vertexium.elasticsearch5.scoring;

import org.elasticsearch.index.query.QueryBuilder;
import org.vertexium.Graph;
import org.vertexium.PropertyDefinition;
import org.vertexium.TextIndexHint;
import org.vertexium.elasticsearch5.Elasticsearch5SearchIndex;
import org.vertexium.query.QueryParameters;
import org.vertexium.scoring.ScoringStrategy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public interface ElasticsearchScoringStrategy extends ScoringStrategy {
    QueryBuilder updateElasticsearchQuery(
            Graph graph,
            Elasticsearch5SearchIndex searchIndex,
            QueryBuilder query,
            QueryParameters queryParameters
    );

    default List<String> getFieldNames(
            Graph graph,
            Elasticsearch5SearchIndex searchIndex,
            QueryParameters queryParameters,
            String field
    ) {
        PropertyDefinition propertyDefinition = graph.getPropertyDefinition(field);
        if (propertyDefinition == null) {
            return null;
        }
        if (!searchIndex.isPropertyInIndex(graph, field)) {
            return null;
        }
        if (!propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
            return null;
        }

        String[] propertyNames = searchIndex.getPropertyNames(
                graph,
                propertyDefinition.getPropertyName(),
                queryParameters.getAuthorizations()
        );
        return Arrays.stream(propertyNames).map(propertyName ->
                propertyName + (propertyDefinition.getDataType() == String.class ? Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX : "")
        ).collect(Collectors.toList());
    }
}
