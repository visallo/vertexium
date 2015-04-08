package org.vertexium.blueprints;

import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class VertexiumBlueprintsKeyIndexableGraphTestBase extends KeyIndexableGraphTestSuite {
    protected VertexiumBlueprintsKeyIndexableGraphTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
