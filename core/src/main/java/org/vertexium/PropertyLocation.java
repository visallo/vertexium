package org.vertexium;

import java.util.Objects;

public class PropertyLocation implements ElementLocation {
    private final ElementType elementType;
    private final String elementId;
    private final Visibility elementVisibility;
    private final String propertyKey;
    private final String propertyName;
    private final Visibility propertyVisibility;

    public PropertyLocation(
        ElementLocation elementLocation,
        String propertyKey,
        String propertyName,
        Visibility propertyVisibility
    ) {
        this(
            elementLocation.getElementType(),
            elementLocation.getId(),
            elementLocation.getVisibility(),
            propertyKey,
            propertyName,
            propertyVisibility
        );
    }

    public PropertyLocation(
        ElementType elementType,
        String elementId,
        Visibility elementVisibility,
        String propertyKey,
        String propertyName,
        Visibility propertyVisibility
    ) {
        this.elementType = elementType;
        this.elementId = elementId;
        this.elementVisibility = elementVisibility;
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyVisibility = propertyVisibility;
    }

    @Override
    public Visibility getVisibility() {
        return elementVisibility;
    }

    @Override
    public ElementType getElementType() {
        return elementType;
    }

    @Override
    public String getId() {
        return elementId;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PropertyLocation that = (PropertyLocation) o;
        return elementType == that.elementType &&
            elementId.equals(that.elementId) &&
            elementVisibility.equals(that.elementVisibility) &&
            propertyKey.equals(that.propertyKey) &&
            propertyName.equals(that.propertyName) &&
            propertyVisibility.equals(that.propertyVisibility);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elementType, elementId, elementVisibility, propertyKey, propertyName, propertyVisibility);
    }

    @Override
    public String toString() {
        return "PropertyLocation{" +
            "elementType=" + elementType +
            ", elementId='" + elementId + '\'' +
            ", elementVisibility=" + elementVisibility +
            ", propertyKey='" + propertyKey + '\'' +
            ", propertyName='" + propertyName + '\'' +
            ", propertyVisibility=" + propertyVisibility +
            '}';
    }
}
