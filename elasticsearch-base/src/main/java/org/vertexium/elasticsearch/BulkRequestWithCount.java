package org.vertexium.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequest;

public class BulkRequestWithCount {
    private BulkRequest bulkRequest;
    private int count;

    public BulkRequestWithCount() {
        clear();
    }

    public BulkRequest getBulkRequest() {
        return bulkRequest;
    }

    public int getCount() {
        return count;
    }

    public void clear() {
        bulkRequest = new BulkRequest();
        count = 0;
    }

    public void incrementCount() {
        count++;
    }
}
