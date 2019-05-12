package org.vertexium.elasticsearch5.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.vertexium.ElementLocation;

public class IndexBulkItem extends BulkItem {
    public IndexBulkItem(ElementLocation elementLocation, IndexRequest indexRequest) {
        super(indexRequest.index(), elementLocation, indexRequest);
    }
}
