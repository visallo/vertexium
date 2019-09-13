package org.vertexium.elasticsearch5.bulk;

import org.elasticsearch.action.update.UpdateRequest;
import org.vertexium.ElementLocation;

public class UpdateBulkItem extends BulkItem {
    private final String extendedDataTableName;
    private final String extendedDataRowId;

    public UpdateBulkItem(
        ElementLocation elementLocation,
        String extendedDataTableName,
        String extendedDataRowId,
        UpdateRequest updateRequest
    ) {
        super(updateRequest.index(), elementLocation, updateRequest);
        this.extendedDataTableName = extendedDataTableName;
        this.extendedDataRowId = extendedDataRowId;
    }

    public ElementLocation getElementLocation() {
        return (ElementLocation) getElementId();
    }

    @Override
    public String toString() {
        String elementId = getElementId().getId();
        if (extendedDataRowId == null) {
            return String.format("Element \"%s\"", elementId);
        } else {
            return String.format("Extended data row \"%s\":\"%s\":\"%s\"", elementId, extendedDataTableName, extendedDataRowId);
        }
    }

    public String getExtendedDataTableName() {
        return extendedDataTableName;
    }

    public String getExtendedDataRowId() {
        return extendedDataRowId;
    }
}
