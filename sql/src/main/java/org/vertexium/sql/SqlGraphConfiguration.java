package org.vertexium.sql;

import com.google.common.base.Preconditions;
import org.apache.commons.dbcp2.BasicDataSource;
import org.vertexium.VertexiumSerializer;
import org.vertexium.inmemory.InMemoryEdge;
import org.vertexium.inmemory.InMemoryGraphConfiguration;
import org.vertexium.inmemory.InMemoryTableElement;
import org.vertexium.inmemory.InMemoryVertex;
import org.vertexium.sql.collections.SqlMap;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

public class SqlGraphConfiguration extends InMemoryGraphConfiguration {
    protected static final String CONFIG_JDBC_DRIVER_CLASS = "sql.jdbc.driverClass";
    protected static final String CONFIG_JDBC_URL = "sql.jdbc.url";
    protected static final String CONFIG_JDBC_USERNAME = "sql.jdbc.username";
    protected static final String CONFIG_JDBC_PASSWORD = "sql.jdbc.password";
    protected static final String CONFIG_JMX_NAME = "sql.jmxName";
    protected static final String KEY_COLUMN_NAME = "id";
    protected static final String VALUE_COLUMN_NAME = "object";
    protected static final String VERTEX_TABLE_NAME = "vertex";
    protected static final String EDGE_TABLE_NAME = "edge";
    protected static final String METADATA_TABLE_NAME = "metadata";
    protected static final String STREAMING_PROPERTIES_TABLE_NAME = "streaming_properties";
    protected static final String IN_VERTEX_ID_COLUMN = "in_vertex_id";
    protected static final String OUT_VERTEX_ID_COLUMN = "out_vertex_id";

    private final BasicDataSource dataSource;
    private final VertexiumSerializer serializer;

    public SqlGraphConfiguration(Map<String, Object> config) {
        super(config);
        String driverClass = getConfigString(config, CONFIG_JDBC_DRIVER_CLASS);
        String url = getConfigString(config, CONFIG_JDBC_URL);
        String username = getConfigString(config, CONFIG_JDBC_USERNAME);
        String password = getConfigString(config, CONFIG_JDBC_PASSWORD);
        String jmxName = (String) config.get(CONFIG_JMX_NAME);
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(driverClass);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setJmxName(jmxName);
        serializer = createSerializer();
    }

    protected DataSource getDataSource() {
        return dataSource;
    }

    protected String tableNameWithPrefix(String tableName) {
        return getTableNamePrefix() + "_" + tableName;
    }

    protected SqlMap<InMemoryTableElement<InMemoryEdge>> newEdgeMap() {
        return new SqlMap<InMemoryTableElement<InMemoryEdge>>(
                tableNameWithPrefix(EDGE_TABLE_NAME), KEY_COLUMN_NAME, VALUE_COLUMN_NAME, dataSource, serializer) {

            @Override
            protected Map<String, Object> additionalColumns(String key, InMemoryTableElement<InMemoryEdge> value) {
                SqlTableEdge edge = (SqlTableEdge) value;
                Map<String, Object> columns = new HashMap<>();
                columns.put(IN_VERTEX_ID_COLUMN, edge.inVertexId());
                columns.put(OUT_VERTEX_ID_COLUMN, edge.outVertexId());
                return columns;
            }
        };
    }

    protected SqlMap<InMemoryTableElement<InMemoryVertex>> newVertexMap() {
        return new SqlMap<>(
                tableNameWithPrefix(VERTEX_TABLE_NAME), KEY_COLUMN_NAME, VALUE_COLUMN_NAME, dataSource, serializer);
    }

    protected SqlMap<Object> newMetadataMap() {
        return new SqlMap<>(
                tableNameWithPrefix(METADATA_TABLE_NAME), KEY_COLUMN_NAME, VALUE_COLUMN_NAME, dataSource, serializer);
    }

    protected SqlStreamingPropertyTable newStreamingPropertyTable() {
        return new SqlStreamingPropertyTable(tableNameWithPrefix(STREAMING_PROPERTIES_TABLE_NAME), dataSource);
    }

    private static String getConfigString(Map<String, Object> config, String key) {
        String value = (String) config.get(key);
        Preconditions.checkNotNull(value, "config property '" + key + "' is required");
        return value;
    }
}
