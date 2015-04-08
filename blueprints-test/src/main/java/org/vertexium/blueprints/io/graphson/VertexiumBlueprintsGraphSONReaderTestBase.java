package org.vertexium.blueprints.io.graphson;

import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;

public abstract class VertexiumBlueprintsGraphSONReaderTestBase extends GraphSONReaderTestSuite {
    protected VertexiumBlueprintsGraphSONReaderTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
