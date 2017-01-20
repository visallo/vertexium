package org.vertexium.mutation;

import org.vertexium.Visibility;
import org.vertexium.util.IncreasingTime;

public class KeyNameVisibilityPropertySoftDeleteMutation extends PropertySoftDeleteMutation {
    private final String key;
    private final String name;
    private final Visibility visibility;
    private final long timestamp;

    public KeyNameVisibilityPropertySoftDeleteMutation(String key, String name, Visibility visibility) {
        this.key = key;
        this.name = name;
        this.visibility = visibility;
        this.timestamp = IncreasingTime.currentTimeMillis();
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    public Visibility getVisibility() {
        return visibility;
    }
}
