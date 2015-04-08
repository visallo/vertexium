package org.vertexium.elasticsearch;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseFilterBuilder;

import java.io.IOException;

public class AuthorizationFilterBuilder extends BaseFilterBuilder {
    private final String[] authorizations;

    public AuthorizationFilterBuilder(String[] authorizations) {
        this.authorizations = authorizations;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("authorizations");
        for (String authorization : authorizations) {
            builder.value(authorization);
        }
        builder.endArray();
    }
}
