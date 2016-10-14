package org.vertexium.accumulo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AccumuloGraphTest extends AccumuloGraphTestBase {

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
