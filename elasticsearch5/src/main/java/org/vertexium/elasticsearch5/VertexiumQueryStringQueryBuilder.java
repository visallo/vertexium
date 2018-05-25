package org.vertexium.elasticsearch5;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.vertexium.Authorizations;
import org.vertexium.VertexiumException;

import java.io.IOException;
import java.util.Objects;

public class VertexiumQueryStringQueryBuilder extends QueryStringQueryBuilder {
    public static final String NAME = "vertexium_query_string";
    private final Authorizations authorizations;

    private VertexiumQueryStringQueryBuilder(String queryString, Authorizations authorizations) {
        super(queryString);
        this.authorizations = authorizations;
        allowLeadingWildcard(false);
    }

    public static VertexiumQueryStringQueryBuilder build(String queryString, Authorizations authorizations) {
        return new VertexiumQueryStringQueryBuilder(queryString, authorizations);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeStringArray(authorizations.getAuthorizations());
    }

    @Override
    protected void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("vertexium_query_string");
        super.doXContent(builder, params);

        builder.startArray("authorizations");
        for (String authorization : authorizations.getAuthorizations()) {
            builder.value(authorization);
        }
        builder.endArray();

        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        throw new VertexiumException("not implemented");
    }

    @Override
    protected boolean doEquals(QueryStringQueryBuilder other) {
        return other instanceof VertexiumQueryStringQueryBuilder &&
                super.doEquals(other) &&
                Objects.deepEquals(this.authorizations, ((VertexiumQueryStringQueryBuilder)other).authorizations);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.doHashCode(), authorizations);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
