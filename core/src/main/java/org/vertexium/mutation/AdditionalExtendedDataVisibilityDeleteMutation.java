package org.vertexium.mutation;

import org.vertexium.Visibility;

public class AdditionalExtendedDataVisibilityDeleteMutation {
    private final String tableName;
    private final String row;
    private final Visibility additionalVisibility;
    private final Object eventData;

    public AdditionalExtendedDataVisibilityDeleteMutation(
        String tableName,
        String row,
        Visibility additionalVisibility,
        Object eventData
    ) {
        this.tableName = tableName;
        this.row = row;
        this.additionalVisibility = additionalVisibility;
        this.eventData = eventData;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRow() {
        return row;
    }

    public Visibility getAdditionalVisibility() {
        return additionalVisibility;
    }

    public Object getEventData() {
        return eventData;
    }
}
