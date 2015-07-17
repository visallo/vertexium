package org.vertexium.accumulo;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AccumuloGraphTest extends AccumuloGraphTestBase {

    @Override
    protected String substitutionDeflate(String str) {
        return str;
    }
}
