package org.neolumin.vertexium.accumulo.blueprints;

import org.neolumin.vertexium.accumulo.AccumuloGraph;
import org.neolumin.vertexium.blueprints.AuthorizationsProvider;
import org.neolumin.vertexium.blueprints.VertexiumBlueprintsGraph;
import org.neolumin.vertexium.blueprints.VisibilityProvider;

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
