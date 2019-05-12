package org.vertexium.elasticsearch5.bulk;

import org.elasticsearch.action.ActionRequest;
import org.vertexium.ElementId;
import org.vertexium.elasticsearch5.utils.ElasticsearchRequestUtils;

public class BulkItem {
    private final String[] indexNames;
    private final ElementId elementId;
    private final int size;
    private final ActionRequest actionRequest;
    private long createdOrLastTriedTime;
    private int failCount;

    public BulkItem(
        String indexName,
        ElementId elementId,
        ActionRequest actionRequest
    ) {
        this(new String[]{indexName}, elementId, actionRequest);
    }

    public BulkItem(
        String[] indexNames,
        ElementId elementId,
        ActionRequest actionRequest
    ) {
        this.indexNames = indexNames;
        this.elementId = elementId;
        this.size = ElasticsearchRequestUtils.getSize(actionRequest);
        this.actionRequest = actionRequest;
        this.createdOrLastTriedTime = System.currentTimeMillis();
    }

    public String[] getIndexNames() {
        return indexNames;
    }

    public ElementId getElementId() {
        return elementId;
    }

    public int getSize() {
        return size;
    }

    public ActionRequest getActionRequest() {
        return actionRequest;
    }

    public long getCreatedOrLastTriedTime() {
        return createdOrLastTriedTime;
    }

    public void updateCreatedOrLastTriedTime() {
        this.createdOrLastTriedTime = System.currentTimeMillis();
    }

    public void incrementFailCount() {
        failCount++;
    }

    public int getFailCount() {
        return failCount;
    }

    @Override
    public String toString() {
        return String.format("%s {elementId=%s, actionRequest=%s}", getClass().getSimpleName(), elementId, actionRequest);
    }
}
