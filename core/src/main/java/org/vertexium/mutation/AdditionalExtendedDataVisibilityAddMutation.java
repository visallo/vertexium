package org.vertexium.mutation;

public class AdditionalExtendedDataVisibilityAddMutation {
    private final String tableName;
    private final String row;
    private final String additionalVisibility;
    private final Object eventData;

    public AdditionalExtendedDataVisibilityAddMutation(
        String tableName,
        String row,
        String additionalVisibility,
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

    public String getAdditionalVisibility() {
        return additionalVisibility;
    }

    public Object getEventData() {
        return eventData;
    }
}
