package org.vertexium.accumulo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class AccumuloGraphTest extends AccumuloGraphTestBase {

    @RegisterExtension
    static final AccumuloResource accumuloResource = new AccumuloResource();

    @Override
    public AccumuloResource getAccumuloResource() {
        return accumuloResource;
    }

    @Override
    protected String substitutionDeflate(String str) {
        return str;
    }

    @Test
    public void testTracing() {
        getGraph().traceOn("test");
        try {
            getGraph().getVertex("v1", AUTHORIZATIONS_A);
        } finally {
            getGraph().traceOff();
        }
    }
}
