package org.vertexium.accumulo.blueprints.io.graphson;

import org.vertexium.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.vertexium.blueprints.io.graphson.VertexiumBlueprintsGraphSONReaderTestBase;

public class AccumuloGraphSONReaderTest extends VertexiumBlueprintsGraphSONReaderTestBase {
    public AccumuloGraphSONReaderTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
