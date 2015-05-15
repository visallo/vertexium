package org.vertexium.mutation;

import org.vertexium.Visibility;

public class AlterPropertyVisibility {
    private final String key;
    private final String name;
    private Visibility existingVisibility;
    private final Visibility visibility;

    public AlterPropertyVisibility(String key, String name, Visibility existingVisibility, Visibility visibility) {
        this.key = key;
        this.name = name;
        this.existingVisibility = existingVisibility;
        this.visibility = visibility;
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public Visibility getExistingVisibility() {
        return existingVisibility;
    }

    public void setExistingVisibility(Visibility existingVisibility) {
        this.existingVisibility = existingVisibility;
    }

    public Visibility getVisibility() {
        return visibility;
    }
}
