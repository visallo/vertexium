package org.vertexium.mutation;

import org.vertexium.Visibility;

public class ExtendedDataDeleteMutation extends ExtendedDataMutationBase<ExtendedDataDeleteMutation> {
    public ExtendedDataDeleteMutation(
        String tableName,
        String row,
        String columnName,
        String key,
        Visibility visibility
    ) {
        super(tableName, row, columnName, key, visibility);
    }
}
