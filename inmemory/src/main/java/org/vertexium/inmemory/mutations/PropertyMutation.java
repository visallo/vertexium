package org.vertexium.inmemory.mutations;

import org.vertexium.Property;
import org.vertexium.Visibility;

public abstract class PropertyMutation extends Mutation {
    private final String propertyKey;
    private final String propertyName;
    private final Visibility propertyVisibility;

    protected PropertyMutation(long timestamp, String propertyKey, String propertyName, Visibility propertyVisibility, Visibility visibility) {
        super(timestamp, visibility);
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyVisibility = propertyVisibility;
    }

    protected PropertyMutation(long timestamp, Property property, Visibility visibility) {
        this(timestamp, property.getKey(), property.getName(), property.getVisibility(), visibility);
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

    @Override
    public String toString() {
        return this.getClass().getName() + "{" +
            "propertyKey='" + propertyKey + '\'' +
            ", propertyName='" + propertyName + '\'' +
            ", propertyVisibility=" + propertyVisibility +
            ", timestamp=" + getTimestamp() +
            ", visibility=" + getVisibility() +
            '}';
    }
}
