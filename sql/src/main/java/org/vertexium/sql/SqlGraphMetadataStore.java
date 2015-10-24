package org.vertexium.sql;

import org.vertexium.GraphMetadataEntry;
import org.vertexium.GraphMetadataStore;
import org.vertexium.sql.collections.SqlMap;
import org.vertexium.util.ConvertingIterable;

import java.util.Map;

public class SqlGraphMetadataStore extends GraphMetadataStore {
    private final SqlMap<Object> metadata;

    public SqlGraphMetadataStore(SqlMap<Object> metadata) {
        this.metadata = metadata;
    }

    @Override
    public Object getMetadata(String key) {
        return metadata.get(key);
    }

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        return new ConvertingIterable<Map.Entry<String, Object>, GraphMetadataEntry>(metadata.entrySet()) {
            @Override
            protected GraphMetadataEntry convert(Map.Entry<String, Object> o) {
                return new GraphMetadataEntry(o.getKey(), o.getValue());
            }
        };
    }

    @Override
    public void setMetadata(String key, Object value) {
        metadata.put(key, value);
    }
}
