package org.vertexium.accumulo.tools;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.vertexium.Authorizations;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.CompletableMutation;
import org.vertexium.accumulo.keys.DataTableRowKey;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.vertexium.accumulo.ElementMutationBuilder.EMPTY_TEXT;

/**
 * To run in the Vertexium CLI
 * <p>
 * d = new org.vertexium.accumulo.tools.DeleteHistoricalLegacyStreamingPropertyValueData(g)
 * options = new org.vertexium.accumulo.tools.DeleteHistoricalLegacyStreamingPropertyValueData.Options()
 * options.setDryRun(false)
 * options.setVersionsToKeep(1)
 * d.execute(options, auths)
 */
public class DeleteHistoricalLegacyStreamingPropertyValueData {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(DeleteHistoricalLegacyStreamingPropertyValueData.class);
    private final AccumuloGraph graph;

    public DeleteHistoricalLegacyStreamingPropertyValueData(AccumuloGraph graph) {
        this.graph = graph;
    }

    public void execute(Options options, Authorizations authorizations) {
        try {
            org.apache.accumulo.core.security.Authorizations accumuloAuthorizations = graph.toAccumuloAuthorizations(authorizations);
            Scanner scanner = graph.getConnector().createScanner(graph.getDataTableName(), accumuloAuthorizations);
            BatchWriter writer = graph.getConnector().createBatchWriter(
                graph.getDataTableName(),
                graph.getConfiguration().createBatchWriterConfig()
            );
            String lastRowIdPrefix = null;
            List<Key> rowsToDelete = new ArrayList<>();
            try {
                int rowCount = 0;
                for (Map.Entry<Key, Value> row : scanner) {
                    if (rowCount % 10000 == 0) {
                        writer.flush();
                        LOGGER.debug("looking at row: %s (row count: %d)", row.getKey().getRow().toString(), rowCount);
                    }
                    rowCount++;
                    if (!EMPTY_TEXT.equals(row.getKey().getColumnFamily())) {
                        continue;
                    }
                    if (!EMPTY_TEXT.equals(row.getKey().getColumnQualifier())) {
                        continue;
                    }

                    String rowId = row.getKey().getRow().toString();
                    String[] rowIdParts = rowId.split("" + DataTableRowKey.VALUE_SEPARATOR);
                    if (rowIdParts.length < 3) {
                        continue;
                    }

                    if (lastRowIdPrefix == null || !isSameProperty(lastRowIdPrefix, rowId)) {
                        deleteRows(writer, rowsToDelete, options);
                        rowsToDelete.clear();
                        lastRowIdPrefix = rowIdParts[0]
                            + DataTableRowKey.VALUE_SEPARATOR
                            + rowIdParts[1]
                            + DataTableRowKey.VALUE_SEPARATOR
                            + rowIdParts[2];
                    }
                    rowsToDelete.add(row.getKey());
                }
                deleteRows(writer, rowsToDelete, options);
            } finally {
                writer.flush();
                scanner.close();
            }
        } catch (Exception ex) {
            throw new VertexiumException("Could not delete old SPV data", ex);
        }
    }

    private boolean isSameProperty(String lastRowIdPrefix, String rowId) {
        return rowId.startsWith(lastRowIdPrefix + DataTableRowKey.VALUE_SEPARATOR)
            || lastRowIdPrefix.equals(rowId);
    }

    private void deleteRows(BatchWriter writer, List<Key> rowsToDelete, Options options) throws MutationsRejectedException {
        rowsToDelete.sort(Comparator.comparingLong(Key::getTimestamp));
        int i = 0;
        for (Key key : rowsToDelete) {
            if (i < rowsToDelete.size() - options.getVersionsToKeep()) {
                LOGGER.debug("deleting row: %s", key.getRow().toString());
                if (!options.isDryRun()) {
                    CompletableMutation mutation = new CompletableMutation(key.getRow());
                    mutation.putDelete(
                        key.getColumnFamily(),
                        key.getColumnQualifier(),
                        key.getColumnVisibilityParsed(),
                        key.getTimestamp()
                    );
                    writer.addMutation(mutation);
                }
            } else {
                if (options.isDryRun()) {
                    LOGGER.debug("skipping row: %s", key.getRow().toString());
                }
            }
            i++;
        }
    }

    public static class Options {
        private int versionsToKeep = 1;
        private boolean dryRun = true;

        public int getVersionsToKeep() {
            return versionsToKeep;
        }

        public Options setVersionsToKeep(int versionsToKeep) {
            this.versionsToKeep = versionsToKeep;
            return this;
        }

        public boolean isDryRun() {
            return dryRun;
        }

        public Options setDryRun(boolean dryRun) {
            this.dryRun = dryRun;
            return this;
        }
    }
}
