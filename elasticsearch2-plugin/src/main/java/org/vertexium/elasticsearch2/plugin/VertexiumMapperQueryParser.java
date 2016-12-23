package org.vertexium.elasticsearch2.plugin;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryParseContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VertexiumMapperQueryParser extends MapperQueryParser {
    private final String[] authorizations;
    private final FieldNameToVisibilityMap fieldNameToVisibilityMap;
    private static final Pattern PROPERTY_NAME_PATTERN = Pattern.compile("^(.*?)(_([0-9a-f]{32}))?(_([a-z]))?$");

    public VertexiumMapperQueryParser(
            QueryParseContext parseContext,
            FieldNameToVisibilityMap fieldNameToVisibilityMap,
            String[] authorizations
    ) {
        super(parseContext);
        this.authorizations = authorizations;
        this.fieldNameToVisibilityMap = fieldNameToVisibilityMap;
    }

    @Override
    protected Query newFieldQuery(Analyzer analyzer, String field, String queryText, boolean quoted) throws ParseException {
        if (field == null || field.length() == 0) {
            return super.newFieldQuery(analyzer, field, queryText, quoted);
        }

        Matcher m = PROPERTY_NAME_PATTERN.matcher(field);
        if (m.matches() && m.group(2) != null) {
            return super.newFieldQuery(analyzer, field, queryText, quoted);
        }

        String fieldPrefix = field + "_";
        DisjunctionMaxQuery query = new DisjunctionMaxQuery(0.0f);
        for (String fieldName : fieldNameToVisibilityMap.getFieldNames()) {
            if (fieldName.startsWith(fieldPrefix)) {
                String visibility = fieldNameToVisibilityMap.getFieldVisibility(fieldName);
                if (VisibilityUtils.canRead(visibility, authorizations)) {
                    Query termQuery = super.newFieldQuery(analyzer, fieldName, queryText, quoted);
                    query.add(termQuery);
                }
            }
        }

        if (query.getDisjuncts().size() == 0) {
            return super.newFieldQuery(analyzer, field, queryText, quoted);
        }

        return query;
    }
}
