package org.vertexium.accumulo;

import org.vertexium.property.StreamingPropertyValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

class StreamingPropertyValueTable extends StreamingPropertyValue {
    private final AccumuloGraph graph;
    private final String dataRowKey;
    private final long timestamp;
    private transient byte[] data;

    StreamingPropertyValueTable(AccumuloGraph graph, String dataRowKey, StreamingPropertyValueTableRef valueRef, long timestamp) {
        super(null, valueRef.getValueType());
        this.timestamp = timestamp;
        this.store(valueRef.isStore());
        this.searchIndex(valueRef.isSearchIndex());
        this.graph = graph;
        this.dataRowKey = dataRowKey;
        this.data = valueRef.getData();
    }

    @Override
    public long getLength() {
        ensureDataLoaded();
        return this.data.length;
    }

    public String getDataRowKey() {
        return dataRowKey;
    }

    public boolean isDataLoaded() {
        return this.data != null;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public InputStream getInputStream() {
        // we need to store the data here to handle the case that the mutation hasn't been flushed yet but the element is
        // passed to the search indexer to be indexed and we can't get the value yet.
        ensureDataLoaded();
        return new ByteArrayInputStream(this.data);
    }

    private void ensureDataLoaded() {
        if (!isDataLoaded()) {
            this.data = this.graph.streamingPropertyValueTableData(this.dataRowKey, this.timestamp);
        }
    }
}
