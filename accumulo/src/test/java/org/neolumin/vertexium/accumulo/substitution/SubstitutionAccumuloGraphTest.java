package org.neolumin.vertexium.accumulo.substitution;

import org.apache.accumulo.core.client.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neolumin.vertexium.*;
import org.neolumin.vertexium.accumulo.AccumuloAuthorizations;
import org.neolumin.vertexium.accumulo.AccumuloGraphTest;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

@RunWith(JUnit4.class)
public class SubstitutionAccumuloGraphTest extends AccumuloGraphTest {

    @Override
    protected Graph createGraph() throws AccumuloSecurityException, AccumuloException, VertexiumException, InterruptedException, IOException, URISyntaxException {
        return SubstitutionAccumuloGraph.create(createConfiguration());
    }

    @Override
    protected Connector createConnector() throws AccumuloSecurityException, AccumuloException {
        return createConfiguration().createConnector();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new AccumuloAuthorizations(auths);
    }

    @Override
    protected Map createConfig() {
        Map configMap = super.createConfig();
        configMap.put(SubstitutionAccumuloGraphConfiguration.SUBSTITUTION_MAP_PREFIX + ".0.key",  "this_is_a_long_key");
        configMap.put(SubstitutionAccumuloGraphConfiguration.SUBSTITUTION_MAP_PREFIX + ".0.value",  "__tialk__");
        configMap.put(SubstitutionAccumuloGraphConfiguration.SUBSTITUTION_MAP_PREFIX + ".1.key", "this_is_a_long_name");
        configMap.put(SubstitutionAccumuloGraphConfiguration.SUBSTITUTION_MAP_PREFIX + ".1.value",  "__tialn__");
        return configMap;
    }

    private SubstitutionAccumuloGraphConfiguration createConfiguration(){
        return new SubstitutionAccumuloGraphConfiguration(createConfig());
    }
}
