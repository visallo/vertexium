package org.vertexium.sql;

import org.vertexium.VertexiumException;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlGraphDDL {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(SqlGraphDDL.class);
    private static final String BIG_CHAR_COLUMN_TYPE = "clob";
    private static final String BIG_BIN_COLUMN_TYPE = "blob";
    private static final String GET_TABLES_TABLE_NAME_COLUMN = "TABLE_NAME";

    public static void create(DataSource dataSource, SqlGraphConfiguration graphConfig) {
        createMapTable(
                dataSource,
                graphConfig.tableNameWithPrefix(SqlGraphConfiguration.VERTEX_TABLE_NAME),
                BIG_CHAR_COLUMN_TYPE
        );
        createMapTable(
                dataSource,
                graphConfig.tableNameWithPrefix(SqlGraphConfiguration.EDGE_TABLE_NAME),
                BIG_CHAR_COLUMN_TYPE,
                "in_vertex_id varchar(100), out_vertex_id varchar(100)"
        );
        createMapTable(
                dataSource,
                graphConfig.tableNameWithPrefix(SqlGraphConfiguration.METADATA_TABLE_NAME),
                BIG_CHAR_COLUMN_TYPE
        );
        createStreamingPropertiesTable(
                dataSource,
                graphConfig.tableNameWithPrefix(SqlGraphConfiguration.STREAMING_PROPERTIES_TABLE_NAME)
        );
    }

    private static void createMapTable(DataSource dataSource, String tableName, String valueColumnType) {
        createMapTable(dataSource, tableName, valueColumnType, "");
    }

    private static void createMapTable(DataSource dataSource, String tableName, String valueColumnType, String additionalColumns) {
        if (!additionalColumns.isEmpty()) {
            additionalColumns = ", " + additionalColumns;
        }
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s (%s varchar(100) primary key, %s %s not null %s)",
                tableName,
                SqlGraphConfiguration.KEY_COLUMN_NAME,
                SqlGraphConfiguration.VALUE_COLUMN_NAME,
                valueColumnType,
                additionalColumns);
        runSql(dataSource, sql, tableName);
    }

    private static void createStreamingPropertiesTable(DataSource dataSource, String tableName) {
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s varchar(100) primary key, %s %s not null, %s varchar(100) not null, %s bigint not null)",
                tableName,
                SqlStreamingPropertyTable.KEY_COLUMN_NAME,
                SqlStreamingPropertyTable.VALUE_COLUMN_NAME,
                BIG_BIN_COLUMN_TYPE,
                SqlStreamingPropertyTable.VALUE_TYPE_COLUMN_NAME,
                SqlStreamingPropertyTable.VALUE_LENGTH_COLUMN_NAME);
        runSql(dataSource, sql, tableName);
    }

    private static void runSql(DataSource dataSource, String sql, String tableName) {
        try {
            try (Connection connection = dataSource.getConnection()) {
                if (!doesTableExist(connection, tableName)) {
                    LOGGER.info("creating table %s (sql: %s)", tableName, sql);
                    try (Statement statement = connection.createStatement()) {
                        statement.execute(sql);
                    }
                }
            }
        } catch (SQLException ex) {
            throw new VertexiumException("Could not create SQL table: " + tableName + " (sql: " + sql + ")", ex);
        }
    }

    private static boolean doesTableExist(Connection connection, String tableName) throws SQLException {
        ResultSet tables = connection.getMetaData().getTables(null, null, "%", null);
        while (tables.next()) {
            if (tableName.equalsIgnoreCase(tables.getString(GET_TABLES_TABLE_NAME_COLUMN))) {
                return true;
            }
        }
        return false;
    }
}
