package org.vertexium.elasticsearch;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.plugins.AbstractPlugin;

public class VertexiumPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "vertexium-plugin";
    }

    @Override
    public String description() {
        return "Vertexium plugin for applying security filters.";
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof IndicesQueriesModule) {
            ((IndicesQueriesModule) module).addFilter(new AuthorizationsFilterParser());
        }
    }
}
