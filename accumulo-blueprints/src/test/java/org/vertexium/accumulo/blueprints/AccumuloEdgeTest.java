package org.vertexium.accumulo.blueprints;

import org.vertexium.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.vertexium.blueprints.VertexiumBlueprintsEdgeTestBase;

public class AccumuloEdgeTest extends VertexiumBlueprintsEdgeTestBase {
    public AccumuloEdgeTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
