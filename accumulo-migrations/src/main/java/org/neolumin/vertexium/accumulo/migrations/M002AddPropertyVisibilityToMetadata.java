package org.neolumin.vertexium.accumulo.migrations;

import org.apache.accumulo.core.client.mapreduce.AccumuloRowInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neolumin.vertexium.accumulo.AccumuloGraph;
import org.neolumin.vertexium.accumulo.ElementMutationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class M002AddPropertyVisibilityToMetadata extends MRMigrationBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(M001MetadataToRows.class);

    public static void main(String[] args) throws Exception {
        run(new M002AddPropertyVisibilityToMetadata(), args);
    }

    @Override
    protected Class<? extends Mapper> getMigrationMapperClass() {
        return MigrationMapper.class;
    }

    @Override
    protected Class getInputFormatClass() {
        return AccumuloRowInputFormat.class;
    }

    public static class MigrationMapper extends MRMigrationMapperBase<Text, PeekingIterator<Map.Entry<Key, Value>>> {
        @Override
        protected void safeMap(Text row, PeekingIterator<Map.Entry<Key, Value>> value, Context context) throws IOException, InterruptedException {
            context.setStatus(row.toString());

            String propertyVisibility = null;
            while (value.hasNext()) {
                Map.Entry<Key, Value> column = value.next();

                if (column.getKey().getColumnFamily().toString().equals("PROP")) {
                    propertyVisibility = AccumuloGraph.accumuloVisibilityToVisibility(column.getKey().getColumnVisibilityParsed()).getVisibilityString();
                } else if (column.getKey().getColumnFamily().toString().equals("PROPMETA")) {
                    updateMetadata(column.getKey(), column.getValue(), propertyVisibility, context);
                }
            }
        }

        private void updateMetadata(Key key, Value value, String propertyVisibility, Context context) throws IOException, InterruptedException {
            String columnQualifier = key.getColumnQualifier().toString();
            if (count(columnQualifier, ElementMutationBuilder.VALUE_SEPARATOR) != 2) {
                return;
            }

            Mutation m = new Mutation(key.getRow());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("mutation: " + key.getRow());
            }

            int lastValueSeparator = columnQualifier.lastIndexOf(ElementMutationBuilder.VALUE_SEPARATOR);
            String newColumnQualifier = columnQualifier.substring(0, lastValueSeparator) + ElementMutationBuilder.VALUE_SEPARATOR + propertyVisibility + columnQualifier.substring(lastValueSeparator);

            m.put(key.getColumnFamily(), new Text(newColumnQualifier), key.getColumnVisibilityParsed(), key.getTimestamp(), value);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("  put: " + key.getColumnFamily() + ", " + newColumnQualifier + ", " + key.getColumnVisibilityParsed() + ", " + key.getTimestamp() + ", " + value);
            }

            m.putDelete(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("  put delete: " + key.getColumnFamily() + ", " + key.getColumnQualifier() + ", " + key.getColumnVisibilityParsed() + ", " + key.getTimestamp());
            }
            context.write(getOutputTableNameText(), m);
        }

        private int count(String columnQualifier, String valueSeparator) {
            int fromIndex = 0;
            int count = 0;
            while (true) {
                fromIndex = columnQualifier.indexOf(valueSeparator, fromIndex);
                if (fromIndex < 0) {
                    break;
                }
                fromIndex++;
                count++;
            }
            return count;
        }
    }
}
