package org.neolumin.vertexium.accumulo.blueprints.io.gml;

import org.neolumin.vertexium.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.neolumin.vertexium.blueprints.io.gml.VertexiumBlueprintsGMLReaderTestBase;

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
