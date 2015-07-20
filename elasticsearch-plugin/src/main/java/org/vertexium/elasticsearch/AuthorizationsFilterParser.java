package org.vertexium.elasticsearch;

import org.apache.lucene.search.FieldValueFilter;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.lucene.search.OrFilter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.FilterParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.vertexium.inmemory.security.Authorizations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AuthorizationsFilterParser implements FilterParser {
    private static final String NAME = "authorizations";

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        Authorizations authorizations = createAuthorizationsFromQuery(parseContext);
        return createFilters(authorizations);
    }

    private Authorizations createAuthorizationsFromQuery(QueryParseContext parseContext) throws IOException {
        List<String> authorizationStrings = getAuthorizationsFromQuery(parseContext);
        return new Authorizations(authorizationStrings.toArray(new String[authorizationStrings.size()]));
    }

    private List<String> getAuthorizationsFromQuery(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_ARRAY) {
            throw new QueryParsingException(parseContext.index(), "authorizations must be an array.");
        }

        List<String> authorizationStrings = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token != XContentParser.Token.VALUE_STRING) {
                throw new QueryParsingException(parseContext.index(), "authorizations must be an array of strings.");
            }

            String authorization = parser.text();
            authorizationStrings.add(authorization);
        }
        return authorizationStrings;
    }

    private FieldValueFilter createVisibilityFieldMissingFilter() {
        return new FieldValueFilter(AuthorizationsFilter.VISIBILITY_FIELD_NAME, true);
    }

    private AuthorizationsFilter createAuthorizationsFilter(Authorizations authorizations) {
        return new AuthorizationsFilter(authorizations);
    }

    private Filter createFilters(Authorizations authorizations) {
        List<Filter> filters = new ArrayList<>();
        filters.add(createVisibilityFieldMissingFilter());
        filters.add(createAuthorizationsFilter(authorizations));
        return new OrFilter(filters);
    }
}
