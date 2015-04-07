package org.neolumin.vertexium.accumulo;

import org.apache.accumulo.core.client.Connector;

public class AccumuloGraphTestUtils {
    public static void ensureTableExists(Connector connector, String tableName) {
        try {
            if (!connector.tableOperations().exists(tableName)) {
                connector.tableOperations().create(tableName, false);
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to create table " + tableName);
        }
    }

    public static void dropGraph(Connector connector, String graphDirectoryName) {
        try {
            if (connector.tableOperations().exists(graphDirectoryName)) {
                connector.tableOperations().delete(graphDirectoryName);
            }
            connector.tableOperations().create(graphDirectoryName, false);
        } catch (Exception e) {
            throw new RuntimeException("Unable to drop graph: " + graphDirectoryName, e);
        }
    }
}
