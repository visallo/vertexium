package org.vertexium.elasticsearch5.plugin;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.singletonList;

public class VertexiumElasticsearchPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>(
            VertexiumQueryStringQueryBuilder.NAME,
            VertexiumQueryStringQueryBuilder::new,
            parseContext -> (Optional<VertexiumQueryStringQueryBuilder>) (Optional) VertexiumQueryStringQueryBuilder.fromXContent(parseContext)
        ));
    }
}
