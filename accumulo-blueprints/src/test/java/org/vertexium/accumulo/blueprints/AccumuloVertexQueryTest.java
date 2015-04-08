package org.vertexium.accumulo.blueprints;

import org.vertexium.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.vertexium.blueprints.VertexiumBlueprintsVertexQueryTestBase;

public class AccumuloVertexQueryTest extends VertexiumBlueprintsVertexQueryTestBase {
    public AccumuloVertexQueryTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
