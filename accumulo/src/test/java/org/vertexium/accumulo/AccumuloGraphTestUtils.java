package org.vertexium.accumulo;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.vertexium.VertexiumException;

public class AccumuloGraphTestUtils {
    public static void ensureTableExists(Connector connector, String tableName) {
        if (!connector.tableOperations().exists(tableName)) {
            createTable(connector, tableName);
        }
    }

    private static void createTable(Connector connector, String tableName) {
        try {
            NewTableConfiguration config = new NewTableConfiguration()
                    .withoutDefaultIterators()
                    .setTimeType(TimeType.MILLIS);
            connector.tableOperations().create(tableName, config);
        } catch (Exception e) {
            throw new VertexiumException("Unable to create table " + tableName);
        }
    }
}
