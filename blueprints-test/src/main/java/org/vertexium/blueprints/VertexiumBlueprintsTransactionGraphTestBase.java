package org.vertexium.blueprints;

import com.tinkerpop.blueprints.TransactionalGraphTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class VertexiumBlueprintsTransactionGraphTestBase extends TransactionalGraphTestSuite {
    protected VertexiumBlueprintsTransactionGraphTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
