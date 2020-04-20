package org.vertexium.accumulo;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.vertexium.VertexiumException;

import java.util.EnumSet;

public class AccumuloGraphTestUtils {
    public static void ensureTableExists(Connector connector, String tableName) {
        if (!connector.tableOperations().exists(tableName)) {
            createTable(connector, tableName, null);
        }
    }

    public static void dropGraph(Connector connector, String tableName) {
        dropGraph(connector, tableName, null);
    }

    public static void dropGraph(Connector connector, String tableName, Integer maxVersions) {
        try {
            if (connector.tableOperations().exists(tableName)) {
                connector.tableOperations().delete(tableName);
            }
        } catch (Exception e) {
            throw new VertexiumException("Unable to drop graph: " + tableName, e);
        }
        createTable(connector, tableName, maxVersions);
    }

    private static void createTable(Connector connector, String tableName, Integer maxVersions) {
        try {
            NewTableConfiguration config = new NewTableConfiguration()
                .withoutDefaultIterators()
                .setTimeType(TimeType.MILLIS);
            connector.tableOperations().create(tableName, config);

            if (maxVersions != null) {
                IteratorSetting versioningSettings = new IteratorSetting(
                    AccumuloGraph.ACCUMULO_DEFAULT_VERSIONING_ITERATOR_PRIORITY,
                    AccumuloGraph.ACCUMULO_DEFAULT_VERSIONING_ITERATOR_NAME,
                    VersioningIterator.class
                );
                VersioningIterator.setMaxVersions(versioningSettings, maxVersions);
                EnumSet<IteratorUtil.IteratorScope> scope = EnumSet.allOf(IteratorUtil.IteratorScope.class);
                connector.tableOperations().attachIterator(tableName, versioningSettings, scope);
            }
        } catch (Exception e) {
            throw new VertexiumException("Unable to create table " + tableName);
        }
    }
}
