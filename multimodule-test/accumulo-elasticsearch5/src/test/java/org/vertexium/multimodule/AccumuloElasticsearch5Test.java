package org.vertexium.multimodule;

import com.google.common.base.Joiner;
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
import org.vertexium.elasticsearch5.ElasticsearchResource;
import org.vertexium.id.SimpleNameSubstitutionStrategy;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.vertexium.id.SimpleSubstitutionUtils.KEY_IDENTIFIER;
import static org.vertexium.id.SimpleSubstitutionUtils.SUBSTITUTION_MAP_PREFIX;
import static org.vertexium.id.SimpleSubstitutionUtils.VALUE_IDENTIFIER;

public class AccumuloElasticsearch5Test extends AccumuloGraphTestBase {
    @ClassRule
    public static final AccumuloResource accumuloResource = new AccumuloResource(new HashMap<String, String>() {{
        put(AccumuloGraphConfiguration.NAME_SUBSTITUTION_STRATEGY_PROP_PREFIX, SimpleNameSubstitutionStrategy.class.getName());
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "0", KEY_IDENTIFIER}), "k1");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "0", VALUE_IDENTIFIER}), "k");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "1", KEY_IDENTIFIER}), "author");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "1", VALUE_IDENTIFIER}), "a");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "2", KEY_IDENTIFIER}), "label");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "2", VALUE_IDENTIFIER}), "l");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "3", KEY_IDENTIFIER}), "http://vertexium.org");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "3", VALUE_IDENTIFIER}), "hvo");
    }});


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
        return true;
    }

    @Override
    protected boolean isAdvancedGeoQuerySupported() {
        return true;
    }

    @Override
    protected boolean disableEdgeIndexing(Graph graph) {
        return elasticsearchResource.disableEdgeIndexing(graph);
    }

    @Override
    protected String substitutionDeflate(String str) {
        if (str.equals("author")) {
            return SimpleNameSubstitutionStrategy.SUBS_DELIM + "a" + SimpleNameSubstitutionStrategy.SUBS_DELIM;
        }
        return str;
    }
}
