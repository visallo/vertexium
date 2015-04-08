package org.vertexium.accumulo.blueprints;

import org.vertexium.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.vertexium.blueprints.VertexiumBlueprintsGraphQueryTestBase;

public class AccumuloGraphQueryTest extends VertexiumBlueprintsGraphQueryTestBase {
    public AccumuloGraphQueryTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
