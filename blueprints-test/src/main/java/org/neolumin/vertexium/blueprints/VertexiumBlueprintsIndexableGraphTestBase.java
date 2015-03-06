package org.neolumin.vertexium.blueprints;

import com.tinkerpop.blueprints.IndexableGraphTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class VertexiumBlueprintsIndexableGraphTestBase extends IndexableGraphTestSuite {
    protected VertexiumBlueprintsIndexableGraphTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
