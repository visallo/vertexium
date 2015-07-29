package org.vertexium;

import org.vertexium.util.JavaSerializableUtils;

public class GraphMetadataEntry {
    private String key;
    private byte[] valueData;
    private volatile Object value;

    public GraphMetadataEntry(String key, byte[] valueData) {
        this.key = key;
        this.valueData = valueData;
    }

    public GraphMetadataEntry(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        if (value != null) {
            return value;
        }
        return value = JavaSerializableUtils.bytesToObject(valueData);
    }

    @Override
    public String toString() {
        return "GraphMetadataEntry{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}
