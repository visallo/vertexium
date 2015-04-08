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
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_ARRAY) {
            throw new QueryParsingException(parseContext.index(), "authorizations must be an array.");
        }

        List<String> authorizationStrings = new ArrayList<String>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token != XContentParser.Token.VALUE_STRING) {
                throw new QueryParsingException(parseContext.index(), "authorizations must be an array of strings.");
            }

            String authorization = parser.text();
            authorizationStrings.add(authorization);
        }

        Authorizations authorizations = new Authorizations(authorizationStrings.toArray(new String[authorizationStrings.size()]));

        List<Filter> filters = new ArrayList<Filter>();
        filters.add(new FieldValueFilter(AuthorizationsFilter.VISIBILITY_FIELD_NAME, true));
        filters.add(new AuthorizationsFilter(authorizations));
        return new OrFilter(filters);
    }
}
