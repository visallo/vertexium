package org.neolumin.vertexium;

import java.io.Serializable;
import java.util.Set;

public class PropertyDefinition implements Serializable {
    private static final long serialVersionUID = 42L;
    private final String propertyName;
    private final Class dataType;
    private final Set<TextIndexHint> textIndexHints;
    private final Double boost;

    public PropertyDefinition(
            String propertyName,
            Class dataType,
            Set<TextIndexHint> textIndexHints) {
        this(
                propertyName,
                dataType,
                textIndexHints,
                null);
    }

    public PropertyDefinition(
            String propertyName,
            Class dataType,
            Set<TextIndexHint> textIndexHints,
            Double boost) {
        this.propertyName = propertyName;
        this.dataType = dataType;
        this.textIndexHints = textIndexHints;
        this.boost = boost;
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

    @Override
    public String toString() {
        return "PropertyDefinition{" +
                "propertyName='" + propertyName + '\'' +
                ", dataType=" + dataType +
                ", textIndexHints=" + textIndexHints +
                ", boost=" + boost +
                '}';
    }
}
