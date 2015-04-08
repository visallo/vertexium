package org.vertexium.blueprints;

import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class VertexiumBlueprintsEdgeTestBase extends EdgeTestSuite {
    protected VertexiumBlueprintsEdgeTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
