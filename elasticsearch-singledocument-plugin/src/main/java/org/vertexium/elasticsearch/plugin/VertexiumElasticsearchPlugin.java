package org.vertexium.elasticsearch.plugin;

import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.plugins.AbstractPlugin;

public class VertexiumElasticsearchPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "Vertexium";
    }

    @Override
    public String description() {
        return "Vertexium secure graph database plugin";
    }

    public void onModule(IndicesQueriesModule module) {
        module.addQuery(VertexiumQueryStringQueryParser.class);
    }
}
