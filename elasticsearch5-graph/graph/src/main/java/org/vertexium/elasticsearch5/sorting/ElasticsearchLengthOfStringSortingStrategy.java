package org.vertexium.elasticsearch5.sorting;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.vertexium.PropertyDefinition;
import org.vertexium.elasticsearch5.Elasticsearch5Graph;
import org.vertexium.elasticsearch5.FieldNames;
import org.vertexium.elasticsearch5.VertexiumScriptConstants;
import org.vertexium.query.QueryParameters;
import org.vertexium.query.SortDirection;
import org.vertexium.sorting.LengthOfStringSortingStrategy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticsearchLengthOfStringSortingStrategy
    extends LengthOfStringSortingStrategy
    implements ElasticsearchSortingStrategy {

    public ElasticsearchLengthOfStringSortingStrategy(String propertyName) {
        super(propertyName);
    }

    @Override
    public void updateElasticsearchQuery(
        Elasticsearch5Graph graph,
        SearchRequestBuilder q,
        QueryParameters parameters,
        SortDirection direction
    ) {
        PropertyDefinition propertyDefinition = graph.getPropertyDefinition(getPropertyName());

        SortOrder esOrder = direction == SortDirection.ASCENDING ? SortOrder.ASC : SortOrder.DESC;
        Map<String, Object> scriptParams = new HashMap<>();
        String[] propertyNames = graph.getPropertyNames(getPropertyName(), parameters.getUser());
        List<String> fieldNames = Arrays.stream(propertyNames)
            .map(propertyName -> {
                String suffix = propertyDefinition.getDataType() == String.class
                    ? FieldNames.EXACT_MATCH_PROPERTY_NAME_SUFFIX
                    : "";
                return propertyName + suffix;
            })
            .collect(Collectors.toList());
        scriptParams.put("fieldNames", fieldNames);
        scriptParams.put("direction", esOrder.name());
        Script script = new Script(
            ScriptType.INLINE,
            VertexiumScriptConstants.SCRIPT_LANG,
            VertexiumScriptConstants.ScriptId.LENGTH_OF_STRING.name(),
            scriptParams
        );
        ScriptSortBuilder.ScriptSortType sortType = ScriptSortBuilder.ScriptSortType.NUMBER;
        q.addSort(SortBuilders.scriptSort(script, sortType).order(SortOrder.ASC));
    }
}
