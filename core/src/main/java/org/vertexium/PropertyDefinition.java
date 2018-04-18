package org.vertexium;

import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

public class PropertyDefinition implements Serializable {
    private static final long serialVersionUID = 42L;
    private static final PropertyDefinition ID_PROPERTY_DEFINITION = new PropertyDefinition(
            Element.ID_PROPERTY_NAME,
            String.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition LABEL_PROPERTY_DEFINITION = new PropertyDefinition(
            Edge.LABEL_PROPERTY_NAME,
            String.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition OUT_VERTEX_ID_PROPERTY_DEFINITION = new PropertyDefinition(
            Edge.OUT_VERTEX_ID_PROPERTY_NAME,
            String.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition IN_VERTEX_ID_PROPERTY_DEFINITION = new PropertyDefinition(
            Edge.IN_VERTEX_ID_PROPERTY_NAME,
            String.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
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
        // to return the correct values for aggregations we need the original value. The only way to get this is to look
        // at the original text stored in the full text. The exact match index only contains lower cased values.
        if (textIndexHints != null && textIndexHints.contains(TextIndexHint.EXACT_MATCH)) {
            this.textIndexHints.add(TextIndexHint.FULL_TEXT);
        }
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
        if (Element.ID_PROPERTY_NAME.equals(propertyName)) {
            return ID_PROPERTY_DEFINITION;
        }
        if (Edge.LABEL_PROPERTY_NAME.equals(propertyName)) {
            return LABEL_PROPERTY_DEFINITION;
        }
        if (Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            return OUT_VERTEX_ID_PROPERTY_DEFINITION;
        }
        if (Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            return IN_VERTEX_ID_PROPERTY_DEFINITION;
        }
        for (PropertyDefinition propertyDefinition : propertyDefinitions) {
            if (propertyDefinition.getPropertyName().equals(propertyName)) {
                return propertyDefinition;
            }
        }
        throw new VertexiumPropertyNotDefinedException("Could not find property definition for property name: " + propertyName);
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
