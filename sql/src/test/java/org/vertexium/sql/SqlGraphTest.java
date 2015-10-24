package org.vertexium.sql;

import com.google.common.collect.ImmutableMap;
import org.h2.Driver;
import org.h2.store.fs.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.GraphFactory;
import org.vertexium.id.UUIDIdGenerator;
import org.vertexium.inmemory.InMemoryAuthorizations;
import org.vertexium.search.DefaultSearchIndex;
import org.vertexium.test.GraphTestBase;

import javax.sql.DataSource;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@RunWith(JUnit4.class)
public class SqlGraphTest extends GraphTestBase {
    private static final String BIG_CHAR_COLUMN_TYPE = "clob";
    private static final String BIG_BIN_COLUMN_TYPE = "blob";

    private Path dbTempDir;
    private Map<String, String> config;

    @Before
    public void before() throws Exception {
        dbTempDir = Files.createTempDirectory(SqlGraphTest.class.getName());
        String dbFilePath = new File(dbTempDir.toFile(), "test.h2").getAbsolutePath();

        config = new HashMap<>();
        config.put("", SqlGraph.class.getName());
        config.put(SqlGraphConfiguration.CONFIG_JDBC_URL, "jdbc:h2:file:" + dbFilePath);
        config.put(SqlGraphConfiguration.CONFIG_JDBC_DRIVER_CLASS, Driver.class.getName());
        config.put(SqlGraphConfiguration.CONFIG_JDBC_USERNAME, "username");
        config.put(SqlGraphConfiguration.CONFIG_JDBC_PASSWORD, "password");
        config.put(GraphConfiguration.IDGENERATOR_PROP_PREFIX, UUIDIdGenerator.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, DefaultSearchIndex.class.getName());

        SqlGraphConfiguration graphConfig = new SqlGraphConfiguration(ImmutableMap.<String, Object>copyOf(config));
        DataSource dataSource = graphConfig.getDataSource();
        createMapTable(dataSource, graphConfig.tableNameWithPrefix(SqlGraphConfiguration.VERTEX_TABLE_NAME),
                BIG_CHAR_COLUMN_TYPE);
        createMapTable(dataSource, graphConfig.tableNameWithPrefix(SqlGraphConfiguration.EDGE_TABLE_NAME),
                BIG_CHAR_COLUMN_TYPE, "in_vertex_id varchar(100), out_vertex_id varchar(100)");
        createMapTable(dataSource, graphConfig.tableNameWithPrefix(SqlGraphConfiguration.METADATA_TABLE_NAME),
                BIG_CHAR_COLUMN_TYPE);
        createStreamingPropertiesTable(dataSource,
                graphConfig.tableNameWithPrefix(SqlGraphConfiguration.STREAMING_PROPERTIES_TABLE_NAME));
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
        FileUtils.deleteRecursive(dbTempDir.toString(), false);
    }

    private void createMapTable(DataSource dataSource, String tableName, String valueColumnType) throws SQLException {
        createMapTable(dataSource, tableName, valueColumnType, "");
    }

    private void createMapTable(DataSource dataSource, String tableName, String valueColumnType, String additionalColumns)
            throws SQLException {
        if (!additionalColumns.isEmpty()) {
            additionalColumns = ", " + additionalColumns;
        }
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(String.format("create table %s (%s varchar(100) primary key, %s %s not null %s)",
                            tableName,
                            SqlGraphConfiguration.KEY_COLUMN_NAME,
                            SqlGraphConfiguration.VALUE_COLUMN_NAME,
                            valueColumnType,
                            additionalColumns));
            }
        }
    }

    private void createStreamingPropertiesTable(DataSource dataSource, String tableName) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(String.format(
                        "create table %s (%s varchar(100) primary key, %s %s not null, %s varchar(100) not null, %s bigint not null)",
                        tableName,
                        SqlStreamingPropertyTable.KEY_COLUMN_NAME,
                        SqlStreamingPropertyTable.VALUE_COLUMN_NAME,
                        BIG_BIN_COLUMN_TYPE,
                        SqlStreamingPropertyTable.VALUE_TYPE_COLUMN_NAME,
                        SqlStreamingPropertyTable.VALUE_LENGTH_COLUMN_NAME));
            }
        }
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
