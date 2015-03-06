package org.neolumin.vertexium.accumulo.blueprints;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.neolumin.vertexium.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.neolumin.vertexium.blueprints.VertexiumBlueprintsVertexTestBase;

import static com.tinkerpop.blueprints.Direction.*;

public class AccumuloVertexTest extends VertexiumBlueprintsVertexTestBase {
    public AccumuloVertexTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }

    // TODO fix how we interact with blueprints
    // see comments in line on why we are overriding blueprints test
    public void testGettingEdgesAndVertices() {
        Graph graph = graphTest.generateGraph();
        Vertex a = graph.addVertex(null);
        Vertex b = graph.addVertex(null);
        Vertex c = graph.addVertex(null);
        Edge w = graph.addEdge(null, a, b, graphTest.convertLabel("knows"));
        Edge x = graph.addEdge(null, b, c, graphTest.convertLabel("knows"));
        Edge y = graph.addEdge(null, a, c, graphTest.convertLabel("hates"));
        Edge z = graph.addEdge(null, a, b, graphTest.convertLabel("hates"));
        Edge zz = graph.addEdge(null, c, c, graphTest.convertLabel("hates"));

        assertEquals(count(a.getEdges(OUT)), 3);
        assertEquals(count(a.getEdges(OUT, graphTest.convertLabel("hates"))), 2);
        assertEquals(count(a.getEdges(OUT, graphTest.convertLabel("knows"))), 1);
        assertEquals(count(a.getVertices(OUT)), 2); // this was previously 3 because blueprints expects duplicate vertices to be returned multiple times
        assertEquals(count(a.getVertices(OUT, graphTest.convertLabel("hates"))), 2);
        assertEquals(count(a.getVertices(OUT, graphTest.convertLabel("knows"))), 1);
        assertEquals(count(a.getVertices(BOTH)), 2); // this was previously 3 because blueprints expects duplicate vertices to be returned multiple times
        assertEquals(count(a.getVertices(BOTH, graphTest.convertLabel("hates"))), 2);
        assertEquals(count(a.getVertices(BOTH, graphTest.convertLabel("knows"))), 1);

        assertTrue(asList(a.getEdges(OUT)).contains(w));
        assertTrue(asList(a.getEdges(OUT)).contains(y));
        assertTrue(asList(a.getEdges(OUT)).contains(z));
        assertTrue(asList(a.getVertices(OUT)).contains(b));
        assertTrue(asList(a.getVertices(OUT)).contains(c));

        assertTrue(asList(a.getEdges(OUT, graphTest.convertLabel("knows"))).contains(w));
        assertFalse(asList(a.getEdges(OUT, graphTest.convertLabel("knows"))).contains(y));
        assertFalse(asList(a.getEdges(OUT, graphTest.convertLabel("knows"))).contains(z));
        assertTrue(asList(a.getVertices(OUT, graphTest.convertLabel("knows"))).contains(b));
        assertFalse(asList(a.getVertices(OUT, graphTest.convertLabel("knows"))).contains(c));

        assertFalse(asList(a.getEdges(OUT, graphTest.convertLabel("hates"))).contains(w));
        assertTrue(asList(a.getEdges(OUT, graphTest.convertLabel("hates"))).contains(y));
        assertTrue(asList(a.getEdges(OUT, graphTest.convertLabel("hates"))).contains(z));
        assertTrue(asList(a.getVertices(OUT, graphTest.convertLabel("hates"))).contains(b));
        assertTrue(asList(a.getVertices(OUT, graphTest.convertLabel("hates"))).contains(c));

        assertEquals(count(a.getVertices(IN)), 0);
        assertEquals(count(a.getVertices(IN, graphTest.convertLabel("knows"))), 0);
        assertEquals(count(a.getVertices(IN, graphTest.convertLabel("hates"))), 0);
        assertTrue(asList(a.getEdges(OUT)).contains(w));
        assertTrue(asList(a.getEdges(OUT)).contains(y));
        assertTrue(asList(a.getEdges(OUT)).contains(z));

        assertEquals(count(b.getEdges(BOTH)), 3);
        assertEquals(count(b.getEdges(BOTH, graphTest.convertLabel("knows"))), 2);
        assertTrue(asList(b.getEdges(BOTH, graphTest.convertLabel("knows"))).contains(x));
        assertTrue(asList(b.getEdges(BOTH, graphTest.convertLabel("knows"))).contains(w));
        assertTrue(asList(b.getVertices(BOTH, graphTest.convertLabel("knows"))).contains(a));
        assertTrue(asList(b.getVertices(BOTH, graphTest.convertLabel("knows"))).contains(c));

        assertEquals(count(c.getEdges(BOTH, graphTest.convertLabel("hates"))), 2); // this was previously 3 because blueprints expects duplicate vertices to be returned multiple times
        assertEquals(count(c.getVertices(BOTH, graphTest.convertLabel("hates"))), 2); // this was previously 3 because blueprints expects duplicate vertices to be returned multiple times
        assertEquals(count(c.getEdges(BOTH, graphTest.convertLabel("knows"))), 1);
        assertTrue(asList(c.getEdges(BOTH, graphTest.convertLabel("hates"))).contains(y));
        assertTrue(asList(c.getEdges(BOTH, graphTest.convertLabel("hates"))).contains(zz));
        assertTrue(asList(c.getVertices(BOTH, graphTest.convertLabel("hates"))).contains(a));
        assertTrue(asList(c.getVertices(BOTH, graphTest.convertLabel("hates"))).contains(c));
        assertEquals(count(c.getEdges(IN, graphTest.convertLabel("hates"))), 2);
        assertEquals(count(c.getEdges(OUT, graphTest.convertLabel("hates"))), 1);

        try {
            x.getVertex(BOTH);
            fail("Getting edge vertex with direction BOTH should fail");
        } catch (IllegalArgumentException e) {
        } catch (Exception e) {
            fail("Getting edge vertex with direction BOTH should should throw " +
                    IllegalArgumentException.class.getSimpleName());
        }

        graph.shutdown();
    }
}
