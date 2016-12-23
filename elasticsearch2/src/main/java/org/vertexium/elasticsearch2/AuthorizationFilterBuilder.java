package org.vertexium.elasticsearch2;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

public class AuthorizationFilterBuilder extends QueryBuilder {
    private final String[] authorizations;

    public AuthorizationFilterBuilder(String[] authorizations) {
        this.authorizations = authorizations;
    }

    @Override
    protected void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray("authorizations");
        for (String authorization : authorizations) {
            builder.value(authorization);
        }
        builder.endArray();
    }
}
