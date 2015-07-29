package org.vertexium;

import org.vertexium.util.FilterIterable;

public abstract class GraphMetadataStore {
    public abstract Iterable<GraphMetadataEntry> getMetadata();

    public abstract void setMetadata(String key, Object value);

    public Object getMetadata(String key) {
        for (GraphMetadataEntry e : getMetadata()) {
            if (e.getKey().equals(key)) {
                return e.getValue();
            }
        }
        return null;
    }

    public Iterable<GraphMetadataEntry> getMetadataWithPrefix(final String prefix) {
        return new FilterIterable<GraphMetadataEntry>(getMetadata()) {
            @Override
            protected boolean isIncluded(GraphMetadataEntry o) {
                return o.getKey().startsWith(prefix);
            }
        };
    }
}
