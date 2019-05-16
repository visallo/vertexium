package org.vertexium.multimodule;

import com.google.common.base.Joiner;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.vertexium.Graph;
import org.vertexium.GraphWithSearchIndex;
import org.vertexium.Vertex;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.AccumuloGraphConfiguration;
import org.vertexium.accumulo.AccumuloGraphTestBase;
import org.vertexium.accumulo.AccumuloResource;
import org.vertexium.elasticsearch5.Elasticsearch5SearchIndex;
import org.vertexium.elasticsearch5.ElasticsearchResource;
import org.vertexium.elasticsearch5.TestElasticsearch5ExceptionHandler;
import org.vertexium.elasticsearch5.scoring.ElasticsearchFieldValueScoringStrategy;
import org.vertexium.elasticsearch5.scoring.ElasticsearchHammingDistanceScoringStrategy;
import org.vertexium.id.SimpleNameSubstitutionStrategy;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.scoring.ScoringStrategy;
import org.vertexium.serializer.kryo.QuickKryoVertexiumSerializer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.vertexium.id.SimpleSubstitutionUtils.*;

public class AccumuloElasticsearch5Test extends AccumuloGraphTestBase {
    @ClassRule
    public static final AccumuloResource accumuloResource = new AccumuloResource(new HashMap<String, String>() {{
        put(AccumuloGraphConfiguration.NAME_SUBSTITUTION_STRATEGY_PROP_PREFIX, SimpleNameSubstitutionStrategy.class.getName());
        put(AccumuloGraphConfiguration.SERIALIZER, QuickKryoVertexiumSerializer.class.getName());
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
    public static final ElasticsearchResource elasticsearchResource = new ElasticsearchResource(AccumuloElasticsearch5Test.class.getName());

    @Before
    @Override
    public void before() throws Exception {
        elasticsearchResource.dropIndices();
        super.before();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Graph createGraph() throws VertexiumException {
        Map accumuloConfig = accumuloResource.createConfig();
        accumuloConfig.putAll(elasticsearchResource.createConfig());
        return AccumuloGraph.create(new AccumuloGraphConfiguration(accumuloConfig));
    }

    private Elasticsearch5SearchIndex getSearchIndex() {
        return (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
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
    protected boolean isPainlessDateMath() {
        return true;
    }

    @Override
    protected String substitutionDeflate(String str) {
        if (str.equals("author")) {
            return SimpleNameSubstitutionStrategy.SUBS_DELIM + "a" + SimpleNameSubstitutionStrategy.SUBS_DELIM;
        }
        return str;
    }

    @Override
    protected ScoringStrategy getHammingDistanceScoringStrategy(String field, String hash) {
        return new ElasticsearchHammingDistanceScoringStrategy(field, hash);
    }

    @Override
    protected ScoringStrategy getFieldValueScoringStrategy(String field) {
        return new ElasticsearchFieldValueScoringStrategy(field);
    }

    @Test
    public void testDocumentMissingHandler() throws Exception {
        TestElasticsearch5ExceptionHandler.authorizations = AUTHORIZATIONS_ALL;

        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.flush();

        elasticsearchResource.clearIndices(getSearchIndex());

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        ExistingElementMutation<Vertex> m = v1.prepareMutation();
        m.setProperty("prop1", "value1", VISIBILITY_A);
        m.save(AUTHORIZATIONS_A);
        graph.flush();
    }
}
