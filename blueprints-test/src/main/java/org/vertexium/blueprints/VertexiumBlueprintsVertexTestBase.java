package org.vertexium.blueprints;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import org.vertexium.Authorizations;
import org.vertexium.Visibility;

public abstract class VertexiumBlueprintsVertexTestBase extends VertexTestSuite {
    protected VertexiumBlueprintsVertexTestBase(GraphTest graphTest) {
        super(graphTest);
    }

    public void testVertexEdgesWithNonVisibleVertexOnOtherEnd() {
        Graph graph = graphTest.generateGraph();

        if (!(graph instanceof VertexiumBlueprintsGraph)) {
            throw new RuntimeException("Invalid graph");
        }
        org.vertexium.Graph vertexiumGraph = ((VertexiumBlueprintsGraph) graph).getGraph();

        Authorizations aAuthorizations = vertexiumGraph.createAuthorizations("a");
        org.vertexium.Vertex v1 = vertexiumGraph.addVertex("v1", new Visibility(""), aAuthorizations);
        org.vertexium.Vertex v2 = vertexiumGraph.addVertex("v2", new Visibility("a"), aAuthorizations);
        org.vertexium.Vertex v3 = vertexiumGraph.addVertex("v3", new Visibility(""), aAuthorizations);
        vertexiumGraph.addEdge("e1to2", v1, v2, "label", new Visibility(""), aAuthorizations);
        vertexiumGraph.addEdge("e1to3", v1, v3, "label", new Visibility(""), aAuthorizations);
        vertexiumGraph.flush();

        Vertex blueV1 = graph.getVertex("v1");
        assertEquals(1, count(blueV1.getEdges(Direction.BOTH, "label")));
        assertEquals(1, count(blueV1.getVertices(Direction.BOTH, "label")));
        assertEquals(1, count((Iterable) blueV1.query().direction(Direction.BOTH).vertexIds()));

        graph.shutdown();
    }
}
