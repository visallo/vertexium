package org.neolumin.vertexium.accumulo.blueprints;

import org.neolumin.vertexium.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.neolumin.vertexium.blueprints.VertexiumBlueprintsEdgeTestBase;

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
