package org.neolumin.vertexium.blueprints;

import com.tinkerpop.blueprints.VertexQueryTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class VertexiumBlueprintsVertexQueryTestBase extends VertexQueryTestSuite {
    protected VertexiumBlueprintsVertexQueryTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
