package org.vertexium.elasticsearch7.bulk;

import org.elasticsearch.action.update.UpdateRequest;
import org.vertexium.ElementLocation;

public class UpdateBulkItem extends BulkItem {
    private final String extendedDataTableName;
    private final String extendedDataRowId;

    public UpdateBulkItem(
        ElementLocation elementLocation,
        UpdateRequest updateRequest
    ) {
        this(elementLocation, null, null, updateRequest);
    }

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

    public String getExtendedDataTableName() {
        return extendedDataTableName;
    }

    public String getExtendedDataRowId() {
        return extendedDataRowId;
    }

    @Override
    public String toString() {
        return String.format(
            "%s {%s:%s%s}",
            UpdateBulkItem.class.getSimpleName(),
            getElementId().getElementType(),
            getElementId().getId(),
            getExtendedDataTableName() == null ? "" : String.format(" (%s:%s)", getExtendedDataTableName(), getExtendedDataRowId())
        );
    }
}
