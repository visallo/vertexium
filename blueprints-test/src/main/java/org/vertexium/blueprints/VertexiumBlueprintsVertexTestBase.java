package org.vertexium.blueprints;

import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class VertexiumBlueprintsVertexTestBase extends VertexTestSuite {
    protected VertexiumBlueprintsVertexTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
