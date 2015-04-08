package org.vertexium.accumulo.blueprints.io.graphml;

import org.vertexium.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.vertexium.blueprints.io.graphml.VertexiumBlueprintsGraphMLReaderTestBase;

public class AccumuloGraphMLReaderTest extends VertexiumBlueprintsGraphMLReaderTestBase {
    public AccumuloGraphMLReaderTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
