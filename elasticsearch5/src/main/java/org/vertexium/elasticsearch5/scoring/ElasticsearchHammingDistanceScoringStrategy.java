package org.vertexium.elasticsearch5.scoring;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.vertexium.Graph;
import org.vertexium.PropertyDefinition;
import org.vertexium.TextIndexHint;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.Elasticsearch5SearchIndex;
import org.vertexium.query.QueryParameters;
import org.vertexium.scoring.HammingDistanceScoringStrategy;
import org.vertexium.util.IOUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticsearchHammingDistanceScoringStrategy
        extends HammingDistanceScoringStrategy
        implements ElasticsearchScoringStrategy {
    private final String scriptSrc;

    public ElasticsearchHammingDistanceScoringStrategy(String field, String hash) {
        super(field, hash);
        try {
            scriptSrc = IOUtils.toString(getClass().getResourceAsStream("hamming-distance.painless"));
        } catch (Exception ex) {
            throw new VertexiumException("Could not load painless script", ex);
        }
    }

    @Override
    public QueryBuilder updateElasticsearchQuery(
            Graph graph,
            Elasticsearch5SearchIndex searchIndex,
            QueryBuilder query,
            QueryParameters queryParameters
    ) {
        PropertyDefinition propertyDefinition = graph.getPropertyDefinition(getField());
        if (propertyDefinition == null) {
            return query;
        }
        if (!searchIndex.isPropertyInIndex(graph, getField())) {
            return query;
        }
        if (!propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
            return query;
        }

        String[] propertyNames = searchIndex.getPropertyNames(
                graph,
                propertyDefinition.getPropertyName(),
                queryParameters.getAuthorizations()
        );
        List<String> fieldNames = Arrays.stream(propertyNames).map(propertyName ->
                propertyName + (propertyDefinition.getDataType() == String.class ? Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX : "")
        ).collect(Collectors.toList());

        HashMap<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("hash", getHash());
        scriptParams.put("fieldNames", fieldNames);
        Script script = new Script(ScriptType.INLINE, "painless", scriptSrc, scriptParams);
        return QueryBuilders.functionScoreQuery(query, new ScriptScoreFunctionBuilder(script));
    }
}
