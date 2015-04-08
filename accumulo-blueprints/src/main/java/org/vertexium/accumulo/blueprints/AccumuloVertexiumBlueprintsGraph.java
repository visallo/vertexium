package org.vertexium.accumulo.blueprints;

import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.blueprints.AuthorizationsProvider;
import org.vertexium.blueprints.VertexiumBlueprintsGraph;
import org.vertexium.blueprints.VisibilityProvider;

public class AccumuloVertexiumBlueprintsGraph extends VertexiumBlueprintsGraph {
    public AccumuloVertexiumBlueprintsGraph(AccumuloGraph graph, VisibilityProvider visibilityProvider, AuthorizationsProvider authorizationsProvider) {
        super(graph, visibilityProvider, authorizationsProvider);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName().toLowerCase() + ":" + getGraph().getConfiguration().getTableNamePrefix();
    }

    @Override
    public AccumuloGraph getGraph() {
        return (AccumuloGraph) super.getGraph();
    }
}
