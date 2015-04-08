package org.vertexium.blueprints;

import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class VertexiumBlueprintsGraphTestBase extends GraphTestSuite {
    protected VertexiumBlueprintsGraphTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
