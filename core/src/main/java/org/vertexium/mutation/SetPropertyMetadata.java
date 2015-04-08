package org.vertexium.mutation;

import org.vertexium.Visibility;

public class SetPropertyMetadata {
    private final String propertyKey;
    private final String propertyName;
    private final Visibility propertyVisibility;
    private final String metadataName;
    private final Object newValue;
    private final Visibility metadataVisibility;

    public SetPropertyMetadata(String propertyKey, String propertyName, Visibility propertyVisibility, String metadataName, Object newValue, Visibility metadataVisibility) {
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyVisibility = propertyVisibility;
        this.metadataName = metadataName;
        this.newValue = newValue;
        this.metadataVisibility = metadataVisibility;
    }

    public String getPropertyKey() {
        return propertyKey;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Visibility getPropertyVisibility() {
        return propertyVisibility;
    }

    public String getMetadataName() {
        return metadataName;
    }

    public Object getNewValue() {
        return newValue;
    }

    public Visibility getMetadataVisibility() {
        return metadataVisibility;
    }
}
