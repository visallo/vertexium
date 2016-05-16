package org.vertexium.sql;

import org.h2.store.fs.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.vertexium.*;
import org.vertexium.id.UUIDIdGenerator;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.search.DefaultSearchIndex;
import org.vertexium.test.GraphTestBase;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@RunWith(JUnit4.class)
public class SqlGraphTest extends GraphTestBase {
    private Path dbTempDir;
    private Map<String, String> config;

    @Before
    public void before() throws Exception {
        dbTempDir = Files.createTempDirectory(SqlGraphTest.class.getName());
        String dbFilePath = new File(dbTempDir.toFile(), "test.h2").getAbsolutePath();

        config = new HashMap<>();
        config.put("", SqlGraph.class.getName());
        config.put("sql.jdbcUrl", "jdbc:h2:file:" + dbFilePath);
        config.put("sql.username", "username");
        config.put("sql.password", "password");
        config.put(GraphConfiguration.IDGENERATOR_PROP_PREFIX, UUIDIdGenerator.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, DefaultSearchIndex.class.getName());
        config.put(GraphConfiguration.SERIALIZER, JavaVertexiumSerializer.class.getName());

        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
        FileUtils.deleteRecursive(dbTempDir.toString(), false);
    }

    @Override
    protected Graph createGraph() {
        return new GraphFactory().createGraph(config);
    }

    @Override
    public SqlGraph getGraph() {
        return (SqlGraph) super.getGraph();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @Override
    protected boolean isEdgeBoostSupported() {
        return false;
    }
}
