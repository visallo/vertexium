package org.neolumin.vertexium.accumulo.blueprints;

import org.neolumin.vertexium.GraphConfiguration;
import org.neolumin.vertexium.VertexiumException;
import org.neolumin.vertexium.accumulo.AccumuloGraph;
import org.neolumin.vertexium.blueprints.*;
import org.neolumin.vertexium.util.ConfigurationUtils;
import org.neolumin.vertexium.util.MapUtils;

import java.util.Map;

public class AccumuloVertexiumBlueprintsGraphFactory extends VertexiumBlueprintsFactory {
    @Override
    public VertexiumBlueprintsGraph createGraph(Map config) {
        AccumuloGraph graph = createAccumuloGraph(config);
        VisibilityProvider visibilityProvider = createVisibilityProvider(graph.getConfiguration());
        AuthorizationsProvider authorizationProvider = createAuthorizationsProvider(graph.getConfiguration());
        return new AccumuloVertexiumBlueprintsGraph(graph, visibilityProvider, authorizationProvider);
    }

    private AccumuloGraph createAccumuloGraph(Map config) {
        try {
            Map graphConfig = MapUtils.getAllWithPrefix(config, "graph");
            return AccumuloGraph.create(graphConfig);
        } catch (Exception ex) {
            throw new VertexiumException("Could not create accumulo graph", ex);
        }
    }

    private VisibilityProvider createVisibilityProvider(GraphConfiguration config) {
        try {
            return ConfigurationUtils.createProvider(config, "visibilityProvider", DefaultVisibilityProvider.class.getName());
        } catch (Exception ex) {
            throw new VertexiumException("Could not create visibility provider", ex);
        }
    }

    private AuthorizationsProvider createAuthorizationsProvider(GraphConfiguration config) {
        try {
            return ConfigurationUtils.createProvider(config, "authorizationsProvider", null);
        } catch (Exception ex) {
            throw new VertexiumException("Could not create authorization provider", ex);
        }
    }
}
