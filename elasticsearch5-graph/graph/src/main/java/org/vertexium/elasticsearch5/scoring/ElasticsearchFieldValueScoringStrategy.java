package org.vertexium.elasticsearch5.scoring;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.vertexium.PropertyDefinition;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.Elasticsearch5Graph;
import org.vertexium.query.QueryParameters;
import org.vertexium.scoring.FieldValueScoringStrategy;
import org.vertexium.util.IOUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticsearchFieldValueScoringStrategy
    extends FieldValueScoringStrategy
    implements ElasticsearchScoringStrategy {
    private final String scriptSrc;

    public ElasticsearchFieldValueScoringStrategy(String field) {
        super(field);
        try {
            scriptSrc = IOUtils.toString(getClass().getResourceAsStream("field-value.painless"));
        } catch (Exception ex) {
            throw new VertexiumException("Could not load painless script", ex);
        }
    }

    @Override
    public QueryBuilder updateElasticsearchQuery(
        Elasticsearch5Graph graph,
        QueryBuilder query,
        QueryParameters queryParameters
    ) {
        List<String> fieldNames = getFieldNames(graph, queryParameters, getField());
        if (fieldNames == null) {
            return query;
        }

        HashMap<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("fieldNames", fieldNames);
        Script script = new Script(ScriptType.INLINE, "painless", scriptSrc, scriptParams);
        return QueryBuilders.functionScoreQuery(query, new ScriptScoreFunctionBuilder(script));
    }

    private List<String> getFieldNames(
        Elasticsearch5Graph graph,
        QueryParameters queryParameters,
        String field
    ) {
        PropertyDefinition propertyDefinition = graph.getPropertyDefinition(field);
        if (propertyDefinition == null) {
            return null;
        }
        if (!graph.isPropertyInIndex(field)) {
            return null;
        }

        String[] propertyNames = graph.getPropertyNames(
            propertyDefinition.getPropertyName(),
            queryParameters.getUser()
        );
        return Arrays.stream(propertyNames)
            .filter(propertyName -> Number.class.isAssignableFrom(propertyDefinition.getDataType()))
            .collect(Collectors.toList());
    }
}
