package org.vertexium.elasticsearch5.plugin;

import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.elasticsearch.index.query.QueryShardContext;
import org.vertexium.elasticsearch5.plugin.utils.VisibilityUtils;

import java.util.Collection;
import java.util.stream.Collectors;

public class VertexiumQueryShardContext extends QueryShardContext {

    private final VertexiumMapperQueryParser queryParser;

    private final FieldNameToVisibilityMap fieldNameToVisibilityMap;

    private final String[] authorizations;

    public VertexiumQueryShardContext(QueryShardContext source, String[] authorizations, FieldNameToVisibilityMap fieldNameToVisibilityMap) {
        super(source);
        this.fieldNameToVisibilityMap = fieldNameToVisibilityMap;
        this.authorizations = authorizations;
        queryParser = new VertexiumMapperQueryParser(this, authorizations, fieldNameToVisibilityMap);
    }

    @Override
    public MapperQueryParser queryParser(QueryParserSettings settings) {
        queryParser.reset(settings);
        return queryParser;
    }

    @Override
    public Collection<String> simpleMatchToIndexNames(String pattern) {
        return super.simpleMatchToIndexNames(pattern).stream().filter(fieldName -> {
            if (fieldName.startsWith("__") && !fieldName.equals(pattern)) {
                return false;
            }
            String visibility = fieldNameToVisibilityMap.getFieldVisibility(fieldName);
            return visibility == null || VisibilityUtils.canRead(visibility, authorizations);
        }).collect(Collectors.toSet());
    }
}
