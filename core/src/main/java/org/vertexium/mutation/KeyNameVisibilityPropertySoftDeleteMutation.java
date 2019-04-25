package org.vertexium.mutation;

import org.vertexium.Visibility;
import org.vertexium.util.IncreasingTime;

public class KeyNameVisibilityPropertySoftDeleteMutation extends PropertySoftDeleteMutation {
    private final String key;
    private final String name;
    private final Visibility visibility;
    private final Object data;
    private final long timestamp;

    public KeyNameVisibilityPropertySoftDeleteMutation(String key, String name, Visibility visibility, Object data) {
        this.key = key;
        this.name = name;
        this.visibility = visibility;
        this.data = data;
        this.timestamp = IncreasingTime.currentTimeMillis();
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public Object getData() {
        return data;
    }
}
