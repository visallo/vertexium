package org.vertexium.test;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.junit.Before;
import org.junit.ClassRule;
import org.vertexium.Graph;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.AccumuloGraphConfiguration;
import org.vertexium.accumulo.AccumuloGraphTestBase;
import org.vertexium.accumulo.AccumuloResource;
import org.vertexium.elasticsearch2.ElasticsearchResource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

public class AccumuloElasticsearch2Test extends AccumuloGraphTestBase {
    @ClassRule
    public static final AccumuloResource accumuloResource = new AccumuloResource();

    @ClassRule
    public static final ElasticsearchResource elasticsearchResource = new ElasticsearchResource();

    @Before
    @Override
    public void before() throws Exception {
        elasticsearchResource.dropIndices();
        super.before();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Graph createGraph() throws AccumuloSecurityException, AccumuloException, VertexiumException, InterruptedException, IOException, URISyntaxException {
        Map accumuloConfig = accumuloResource.createConfig();
        accumuloConfig.putAll(elasticsearchResource.createConfig());
        return AccumuloGraph.create(new AccumuloGraphConfiguration(accumuloConfig));
    }

    @Override
    public AccumuloResource getAccumuloResource() {
        return accumuloResource;
    }

    @Override
    protected boolean isParitalUpdateOfVertexPropertyKeySupported() {
        return false;
    }

    @Override
    protected boolean isFetchHintNoneVertexQuerySupported() {
        return true;
    }

    @Override
    protected boolean isLuceneQueriesSupported() {
        return true;
    }

    @Override
    protected boolean isFieldNamesInQuerySupported() {
        return false;
    }

    @Override
    protected boolean disableEdgeIndexing(Graph graph) {
        return elasticsearchResource.disableEdgeIndexing(graph);
    }

    @Override
    protected String substitutionDeflate(String str) {
        return str;
    }
}
