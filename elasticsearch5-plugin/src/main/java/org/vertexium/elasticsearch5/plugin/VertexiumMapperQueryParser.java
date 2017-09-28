package org.vertexium.elasticsearch5.plugin;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryShardContext;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VertexiumMapperQueryParser extends MapperQueryParser {
    private static final Pattern PROPERTY_NAME_PATTERN = Pattern.compile("^(.*?)(_([0-9a-f]{32}))?(_([a-z]))?$");
    public static final String FIELDNAME_DOT_REPLACEMENT = "-_-";

    private final String[] authorizations;
    private final FieldNameToVisibilityMap fieldNameToVisibilityMap;

    public VertexiumMapperQueryParser(QueryShardContext context, String[] authorizations, FieldNameToVisibilityMap fieldNameToVisibilityMap) {
        super(context);
        this.authorizations = authorizations;
        this.fieldNameToVisibilityMap = fieldNameToVisibilityMap;
    }

    @Override
    protected Query newFieldQuery(Analyzer analyzer, String field, String queryText, boolean quoted) throws ParseException {
        field = field.replace(".", FIELDNAME_DOT_REPLACEMENT);

        if (field == null || field.length() == 0) {
            return super.newFieldQuery(analyzer, field, queryText, quoted);
        }

        Matcher m = PROPERTY_NAME_PATTERN.matcher(field);
        if (m.matches() && m.group(2) != null) {
            String visibility = fieldNameToVisibilityMap.getFieldVisibility(field);
            if (VisibilityUtils.canRead(visibility, authorizations)) {
                return super.newFieldQuery(analyzer, field, queryText, quoted);
            }
            return null;
        }

        String fieldPrefix = field + "_";
        List<Query> disjucts = new ArrayList<>();
        for (String fieldName : fieldNameToVisibilityMap.getFieldNames()) {
            if (fieldName.startsWith(fieldPrefix)) {
                String visibility = fieldNameToVisibilityMap.getFieldVisibility(fieldName);
                if (VisibilityUtils.canRead(visibility, authorizations)) {
                    Query termQuery = super.newFieldQuery(analyzer, fieldName, queryText, quoted);
                    disjucts.add(termQuery);
                }
            }
        }
        DisjunctionMaxQuery query = new DisjunctionMaxQuery(disjucts, 0.0f);

        if (query.getDisjuncts().size() == 0) {
            return super.newFieldQuery(analyzer, field, queryText, quoted);
        }

        return query;
    }
}
