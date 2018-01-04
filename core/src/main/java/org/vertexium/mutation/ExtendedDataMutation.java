package org.vertexium.mutation;

import org.vertexium.Visibility;
import org.vertexium.util.IncreasingTime;

public class ExtendedDataMutation extends ExtendedDataMutationBase<ExtendedDataMutation> {
    private final Object value;
    private final long timestamp;

    public ExtendedDataMutation(
            String tableName,
            String row,
            String columnName,
            String key,
            Object value,
            Long timestamp,
            Visibility visibility
    ) {
        super(tableName, row, columnName, key, visibility);
        this.value = value;
        this.timestamp = timestamp == null ? IncreasingTime.currentTimeMillis() : timestamp;
    }

    public Object getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
