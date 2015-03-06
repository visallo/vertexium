package org.neolumin.vertexium.inmemory;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.Graph;
import org.neolumin.vertexium.GraphConfiguration;
import org.neolumin.vertexium.GraphFactory;
import org.neolumin.vertexium.id.UUIDIdGenerator;
import org.neolumin.vertexium.search.DefaultSearchIndex;
import org.neolumin.vertexium.test.GraphTestBase;

import java.util.HashMap;
import java.util.Map;

@RunWith(JUnit4.class)
public class InMemoryGraphTest extends GraphTestBase {
    @Override
    protected Graph createGraph() {
        Map<String, String> config = new HashMap<>();
        config.put("", InMemoryGraph.class.getName());
        config.put(GraphConfiguration.IDGENERATOR_PROP_PREFIX, UUIDIdGenerator.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, DefaultSearchIndex.class.getName());
        return new GraphFactory().createGraph(config);
    }

    @Override
    public InMemoryGraph getGraph() {
        return (InMemoryGraph) super.getGraph();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @Before
    @Override
    public void before() throws Exception {
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    @Override
    protected boolean isEdgeBoostSupported() {
        return false;
    }
}
