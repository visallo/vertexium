package org.vertexium.elasticsearch7.sorting;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.vertexium.Graph;
import org.vertexium.PropertyDefinition;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch7.Elasticsearch7SearchIndex;
import org.vertexium.query.QueryParameters;
import org.vertexium.query.SortDirection;
import org.vertexium.sorting.LengthOfStringSortingStrategy;
import org.vertexium.util.IOUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticsearchLengthOfStringSortingStrategy
    extends LengthOfStringSortingStrategy
    implements ElasticsearchSortingStrategy {

    public static final String SCRIPT_NAME = "length-of-string.painless";
    private final String scriptSource;

    public ElasticsearchLengthOfStringSortingStrategy(String propertyName) {
        super(propertyName);
        try {
            scriptSource = IOUtils.toString(getClass().getResourceAsStream(SCRIPT_NAME));
        } catch (Exception ex) {
            throw new VertexiumException("Could not load painless script: " + SCRIPT_NAME, ex);
        }
    }

    @Override
    public void updateElasticsearchQuery(
        Graph graph,
        Elasticsearch7SearchIndex searchIndex,
        SearchRequestBuilder q,
        QueryParameters parameters,
        SortDirection direction
    ) {
        PropertyDefinition propertyDefinition = graph.getPropertyDefinition(getPropertyName());

        SortOrder esOrder = direction == SortDirection.ASCENDING ? SortOrder.ASC : SortOrder.DESC;
        Map<String, Object> scriptParams = new HashMap<>();
        String[] propertyNames = searchIndex.getPropertyNames(graph, getPropertyName(), parameters.getAuthorizations());
        List<String> fieldNames = Arrays.stream(propertyNames)
            .map(propertyName -> {
                String suffix = propertyDefinition.getDataType() == String.class
                    ? Elasticsearch7SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX
                    : "";
                return propertyName + suffix;
            })
            .collect(Collectors.toList());
        scriptParams.put("fieldNames", fieldNames);
        scriptParams.put("direction", esOrder.name());
        Script script = new Script(ScriptType.INLINE, "painless", scriptSource, scriptParams);
        ScriptSortBuilder.ScriptSortType sortType = ScriptSortBuilder.ScriptSortType.NUMBER;
        q.addSort(SortBuilders.scriptSort(script, sortType).order(SortOrder.ASC));
    }
}
