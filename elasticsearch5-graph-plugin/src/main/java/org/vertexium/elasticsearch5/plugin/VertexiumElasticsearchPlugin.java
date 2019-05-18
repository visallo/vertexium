package org.vertexium.elasticsearch5.plugin;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.singletonList;

public class VertexiumElasticsearchPlugin extends Plugin implements SearchPlugin, ScriptPlugin {
    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>(
            VertexiumQueryStringQueryBuilder.NAME,
            VertexiumQueryStringQueryBuilder::new,
            parseContext -> (Optional<VertexiumQueryStringQueryBuilder>) (Optional) VertexiumQueryStringQueryBuilder.fromXContent(parseContext)
        ));
    }

    @Override
    public List<NativeScriptFactory> getNativeScripts() {
        List<NativeScriptFactory> results = new ArrayList<>();
        results.add(new VertexiumSaveElementMutationNativeScriptFactory());
        results.add(new VertexiumSaveExtendedDataMutationNativeScriptFactory());
        return results;
    }
}
