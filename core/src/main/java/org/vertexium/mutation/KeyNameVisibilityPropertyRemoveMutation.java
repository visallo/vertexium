package org.vertexium.mutation;

import org.vertexium.Visibility;

public class KeyNameVisibilityPropertyRemoveMutation extends PropertyRemoveMutation {
    private final String key;
    private final String name;
    private final Visibility visibility;

    public KeyNameVisibilityPropertyRemoveMutation(String key, String name, Visibility visibility) {
        this.key = key;
        this.name = name;
        this.visibility = visibility;
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public Visibility getVisibility() {
        return visibility;
    }
}
