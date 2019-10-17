package org.vertexium.accumulo;

import org.junit.ClassRule;
import org.junit.Test;

public class AccumuloGraphTest extends AccumuloGraphTestBase {

    @ClassRule
    public static final AccumuloResource accumuloResource = new AccumuloResource();

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
