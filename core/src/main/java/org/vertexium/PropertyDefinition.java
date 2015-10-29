package org.vertexium;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

public class PropertyDefinition implements Serializable {
    private static final long serialVersionUID = 42L;
    private final String propertyName;
    private final Class dataType;
    private final Set<TextIndexHint> textIndexHints;
    private final Double boost;
    private final boolean sortable;

    public PropertyDefinition(
            String propertyName,
            Class dataType,
            Set<TextIndexHint> textIndexHints) {
        this(
                propertyName,
                dataType,
                textIndexHints,
                null,
                false
        );
    }

    public PropertyDefinition(
            String propertyName,
            Class dataType,
            Set<TextIndexHint> textIndexHints,
            Double boost,
            boolean sortable
    ) {
        this.propertyName = propertyName;
        this.dataType = dataType;
        this.textIndexHints = textIndexHints;
        this.boost = boost;
        this.sortable = sortable;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Class getDataType() {
        return dataType;
    }

    public Set<TextIndexHint> getTextIndexHints() {
        return textIndexHints;
    }

    public Double getBoost() {
        return boost;
    }

    public boolean isSortable() {
        return sortable;
    }

    public static PropertyDefinition findPropertyDefinition(Collection<PropertyDefinition> propertyDefinitions, String propertyName) {
        for (PropertyDefinition propertyDefinition : propertyDefinitions) {
            if (propertyDefinition.getPropertyName().equals(propertyName)) {
                return propertyDefinition;
            }
        }
        throw new VertexiumException("Could not find property definition for property name: " + propertyName);
    }

    @Override
    public String toString() {
        return "PropertyDefinition{" +
                "propertyName='" + propertyName + '\'' +
                ", dataType=" + dataType +
                ", textIndexHints=" + textIndexHints +
                ", boost=" + boost +
                ", sortable=" + sortable +
                '}';
    }
}
