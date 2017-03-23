package org.vertexium.elasticsearch2;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.vertexium.Authorizations;

import java.io.IOException;

public class VertexiumQueryStringQueryBuilder extends QueryBuilder {
    private final String queryString;
    private final Authorizations authorizations;

    private VertexiumQueryStringQueryBuilder(String queryString, Authorizations authorizations) {
        this.queryString = queryString;
        this.authorizations = authorizations;
    }

    public static VertexiumQueryStringQueryBuilder build(String queryString, Authorizations authorizations) {
        return new VertexiumQueryStringQueryBuilder(queryString, authorizations);
    }

    @Override
    protected void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("vertexium_query_string");
        builder.field("query", queryString);

        builder.startArray("authorizations");
        for (String authorization : authorizations.getAuthorizations()) {
            builder.value(authorization);
        }
        builder.endArray();

        builder.endObject();
    }
}
