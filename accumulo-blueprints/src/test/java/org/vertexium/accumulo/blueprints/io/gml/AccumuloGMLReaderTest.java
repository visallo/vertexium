package org.vertexium.accumulo.blueprints.io.gml;

import org.vertexium.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.vertexium.blueprints.io.gml.VertexiumBlueprintsGMLReaderTestBase;

public class AccumuloGMLReaderTest extends VertexiumBlueprintsGMLReaderTestBase {
    public AccumuloGMLReaderTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
