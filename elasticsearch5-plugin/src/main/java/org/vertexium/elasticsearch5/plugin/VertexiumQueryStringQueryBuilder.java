package org.vertexium.elasticsearch5.plugin;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class VertexiumQueryStringQueryBuilder extends QueryStringQueryBuilder {
    public static final String NAME = "vertexium_query_string";
    public static final String ELEMENT_DOCUMENT_MAPPER_NAME = "e";

    private final String[] authorizations;

    public VertexiumQueryStringQueryBuilder(StreamInput in) throws IOException {
        super(in);
        authorizations = in.readStringArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeStringArray(authorizations);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("vertexium_query_string");
        super.doXContent(builder, params);

        builder.startArray("authorizations");
        for (String authorization : authorizations) {
            builder.value(authorization);
        }
        builder.endArray();

        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        FieldNameToVisibilityMap fieldNameToVisibilityMap = getFieldNameToVisibilityMap(context);
        return super.doToQuery(new VertexiumQueryShardContext(context, authorizations, fieldNameToVisibilityMap));
    }

    @Override
    protected boolean doEquals(QueryStringQueryBuilder other) {
        return other instanceof VertexiumQueryStringQueryBuilder &&
                super.doEquals(other) &&
                Objects.deepEquals(this.authorizations, ((VertexiumQueryStringQueryBuilder) other).authorizations);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.doHashCode(), authorizations);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public static Optional<QueryStringQueryBuilder> fromXContent(QueryParseContext parseContext) {
        throw new RuntimeException("not implemented");
    }

    protected FieldNameToVisibilityMap getFieldNameToVisibilityMap(QueryShardContext context) {
        try {
            Map<String, String> results = new HashMap<>();
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings
                    = context.getClient().admin().indices().prepareGetMappings().get().getMappings();
            for (ObjectCursor<String> index : mappings.keys()) {
                ImmutableOpenMap<String, MappingMetaData> types = mappings.get(index.value);
                if (types == null) {
                    continue;
                }
                MappingMetaData elementMetadata = types.get(ELEMENT_DOCUMENT_MAPPER_NAME);
                if (elementMetadata == null) {
                    continue;
                }
                //noinspection unchecked
                Map<String, Map<String, String>> meta = (Map<String, Map<String, String>>) elementMetadata.getSourceAsMap().get("_meta");
                if (meta == null) {
                    continue;
                }
                Map<String, String> vertexiumMeta = meta.get("vertexium");
                if (vertexiumMeta == null) {
                    continue;
                }
                results.putAll(vertexiumMeta);
            }

            return FieldNameToVisibilityMap.createFromVertexiumMetadata(results);
        } catch (IOException ex) {
            throw new RuntimeException("Could not get mappings", ex);
        }
    }
}
