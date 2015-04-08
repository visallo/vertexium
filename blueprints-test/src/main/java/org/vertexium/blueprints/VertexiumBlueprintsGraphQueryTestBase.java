package org.vertexium.blueprints;

import com.tinkerpop.blueprints.GraphQueryTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class VertexiumBlueprintsGraphQueryTestBase extends GraphQueryTestSuite {
    protected VertexiumBlueprintsGraphQueryTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
