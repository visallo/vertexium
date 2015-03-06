package org.neolumin.vertexium.mutation;

import org.neolumin.vertexium.Visibility;

public class AlterPropertyVisibility {
    private final String key;
    private final String name;
    private final Visibility existingVisibility;
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

    public Visibility getVisibility() {
        return visibility;
    }
}
