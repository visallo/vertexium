package org.vertexium.elasticsearch5.plugin;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class VertexiumQueryStringQueryBuilder extends QueryStringQueryBuilder {
    public static final String NAME = "vertexium_query_string";
    public static final String ELEMENT_DOCUMENT_MAPPER_NAME = "element";

    private final String[] authorizations;

    public VertexiumQueryStringQueryBuilder(StreamInput in) throws IOException {
        super(in);
        authorizations = in.readStringArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        FieldNameToVisibilityMap fieldNameToVisibilityMap = getFieldNameToVisibilityMap(context);
        return super.doToQuery(new VertexiumQueryShardContext(context, authorizations, fieldNameToVisibilityMap));
    }

    @Override
    protected boolean doEquals(QueryStringQueryBuilder other) {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected int doHashCode() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public static Optional<QueryStringQueryBuilder> fromXContent(QueryParseContext parseContext) {
        throw new RuntimeException("not implemented");
    }

    protected FieldNameToVisibilityMap getFieldNameToVisibilityMap(QueryShardContext context) {
        Map<String, Object> elementMetadata = context.getMapperService().documentMapper(ELEMENT_DOCUMENT_MAPPER_NAME).meta();
        if (elementMetadata == null) {
            throw new NullPointerException("Could not find " + ELEMENT_DOCUMENT_MAPPER_NAME + " metadata");
        }

        Object vertexiumMeta = elementMetadata.get("vertexium");
        if (vertexiumMeta == null) {
            throw new NullPointerException("Could not find vertexium metadata in field mapping");
        }
        return FieldNameToVisibilityMap.createFromVertexiumMetadata(vertexiumMeta);
    }
}
