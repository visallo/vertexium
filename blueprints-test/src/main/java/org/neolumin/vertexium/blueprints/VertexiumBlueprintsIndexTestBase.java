package org.neolumin.vertexium.blueprints;

import com.tinkerpop.blueprints.IndexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class VertexiumBlueprintsIndexTestBase extends IndexTestSuite {
    protected VertexiumBlueprintsIndexTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
