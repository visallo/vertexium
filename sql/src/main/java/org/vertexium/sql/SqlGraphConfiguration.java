package org.vertexium.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.vertexium.VertexiumSerializer;
import org.vertexium.inmemory.*;
import org.vertexium.sql.collections.SqlMap;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SqlGraphConfiguration extends InMemoryGraphConfiguration {
    protected static final String KEY_COLUMN_NAME = "id";
    protected static final String VALUE_COLUMN_NAME = "object";
    protected static final String VERTEX_TABLE_NAME = "vertex";
    protected static final String EDGE_TABLE_NAME = "edge";
    protected static final String EXTENDED_DATA_TABLE_NAME = "extendeddata";
    protected static final String METADATA_TABLE_NAME = "metadata";
    protected static final String STREAMING_PROPERTIES_TABLE_NAME = "streaming_properties";
    protected static final String IN_VERTEX_ID_COLUMN = "in_vertex_id";
    protected static final String OUT_VERTEX_ID_COLUMN = "out_vertex_id";
    private static final String CONFIG_PREFIX = "sql.";

    private final DataSource dataSource;
    private final VertexiumSerializer serializer;

    public SqlGraphConfiguration(Map<String, Object> config) {
        super(config);
        dataSource = createDataSource(config);
        serializer = createSerializer();
    }

    private DataSource createDataSource(Map<String, Object> config) {
        Properties properties = new Properties();
        for (Map.Entry<String, Object> configEntry : config.entrySet()) {
            String key = configEntry.getKey();
            if (key.startsWith(CONFIG_PREFIX)) {
                key = key.substring(CONFIG_PREFIX.length());
                properties.put(key, configEntry.getValue());
            }
        }
        HikariConfig hikariConfig = new HikariConfig(properties);
        return new HikariDataSource(hikariConfig);
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

    public SqlExtendedDataTable newExtendedDataTable() {
        return new SqlExtendedDataTable(tableNameWithPrefix(EXTENDED_DATA_TABLE_NAME), dataSource, serializer);
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
}
