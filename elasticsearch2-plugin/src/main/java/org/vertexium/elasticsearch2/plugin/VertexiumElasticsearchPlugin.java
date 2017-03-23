package org.vertexium.elasticsearch2.plugin;

import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;

public class VertexiumElasticsearchPlugin extends Plugin {
    @Override
    public String name() {
        return "Vertexium";
    }

    @Override
    public String description() {
        return "Vertexium secure graph database plugin";
    }

    public void onModule(IndicesModule module) {
        module.registerQueryParser(VertexiumQueryStringQueryParser.class);
    }
}
