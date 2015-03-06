package org.neolumin.vertexium;

public class GraphMetadataEntry {
    private String key;
    private Object value;

    public GraphMetadataEntry(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "GraphMetadataEntry{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}
