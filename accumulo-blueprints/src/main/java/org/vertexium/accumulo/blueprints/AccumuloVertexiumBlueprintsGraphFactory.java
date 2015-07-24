package org.vertexium.accumulo.blueprints;

import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.blueprints.*;
import org.vertexium.util.ConfigurationUtils;
import org.vertexium.util.MapUtils;

import java.util.Map;

public class AccumuloVertexiumBlueprintsGraphFactory extends VertexiumBlueprintsFactory {
    @Override
    public VertexiumBlueprintsGraph createGraph(Map config) {
        AccumuloGraph graph = createAccumuloGraph(config);
        VisibilityProvider visibilityProvider = createVisibilityProvider(graph, graph.getConfiguration());
        AuthorizationsProvider authorizationProvider = createAuthorizationsProvider(graph, graph.getConfiguration());
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

    private VisibilityProvider createVisibilityProvider(AccumuloGraph graph, GraphConfiguration config) {
        try {
            return ConfigurationUtils.createProvider(graph, config, "visibilityProvider", DefaultVisibilityProvider.class.getName());
        } catch (Exception ex) {
            throw new VertexiumException("Could not create visibility provider", ex);
        }
    }

    private AuthorizationsProvider createAuthorizationsProvider(AccumuloGraph graph, GraphConfiguration config) {
        try {
            return ConfigurationUtils.createProvider(graph, config, "authorizationsProvider", null);
        } catch (Exception ex) {
            throw new VertexiumException("Could not create authorization provider", ex);
        }
    }
}
