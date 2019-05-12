package org.vertexium.elasticsearch5.plugin;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.query.QueryShardContext;
import org.vertexium.elasticsearch5.plugin.utils.VisibilityUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
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
    protected Query newRegexpQuery(Term regexp) {
        return createQuery(regexp, (term) -> super.newRegexpQuery(term));
    }

    @Override
    protected Query newWildcardQuery(Term t) {
        return createQuery(t, (term) -> super.newWildcardQuery(term));
    }

    @Override
    protected Query newPrefixQuery(Term prefix) {
        return createQuery(prefix, (term) -> super.newPrefixQuery(term));
    }

    @Override
    protected Query newFuzzyQuery(Term term, float minimumSimilarity, int prefixLength) {
        return createQuery(term, (t) -> super.newFuzzyQuery(t, minimumSimilarity, prefixLength));
    }

    @Override
    protected Query newRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
        return createQuery(field, (fieldName) -> super.newRangeQuery(fieldName, part1, part2, startInclusive, endInclusive));
    }

    @Override
    protected Query newFieldQuery(Analyzer analyzer, String field, String queryText, boolean quoted) throws ParseException {
        try {
            return createQuery(field, (fieldName) -> {
                try {
                    return super.newFieldQuery(analyzer, fieldName, queryText, quoted);
                } catch (ParseException e) {
                    throw new RuntimeException("could not create field query", e);
                }
            });
        } catch (RuntimeException ex) {
            if (ex.getCause() != null && ex.getCause() instanceof ParseException) {
                throw (ParseException) ex.getCause();
            }
            throw ex;
        }
    }

    private Query createQuery(Term term, Function<Term, Query> fn) {
        String field = term.field();
        BytesRef value = term.bytes();
        return createQuery(field, (fieldName) -> fn.apply(new Term(fieldName, value)));
    }

    private Query createQuery(String field, Function<String, Query> fn) {
        field = field.replace(".", FIELDNAME_DOT_REPLACEMENT);

        if (field.length() == 0) {
            return fn.apply(field);
        }

        Matcher m = PROPERTY_NAME_PATTERN.matcher(field);
        if (m.matches() && m.group(2) != null) {
            String visibility = fieldNameToVisibilityMap.getFieldVisibility(field);
            if (visibility != null && VisibilityUtils.canRead(visibility, authorizations)) {
                return fn.apply(field);
            }
            return null;
        }

        String fieldPrefix = field + "_";
        List<Query> disjucts = new ArrayList<>();
        for (String fieldName : fieldNameToVisibilityMap.getFieldNames()) {
            if (fieldName.startsWith(fieldPrefix)) {
                String visibility = fieldNameToVisibilityMap.getFieldVisibility(fieldName);
                if (visibility != null && VisibilityUtils.canRead(visibility, authorizations)) {
                    Query termQuery = fn.apply(fieldName);
                    disjucts.add(termQuery);
                }
            }
        }
        DisjunctionMaxQuery query = new DisjunctionMaxQuery(disjucts, 0.0f);

        if (query.getDisjuncts().size() == 0) {
            return fn.apply(field);
        }

        return query;
    }
}
