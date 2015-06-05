package org.vertexium.accumulo.keys;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.vertexium.Property;
import org.vertexium.VertexiumException;
import org.vertexium.Visibility;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.AccumuloNameSubstitutionStrategy;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

public class PropertyColumnQualifier extends KeyBase {
    private static final int PART_INDEX_PROPERTY_NAME = 0;
    private static final int PART_INDEX_PROPERTY_KEY = 1;
    private final String[] parts;

    public PropertyColumnQualifier(Text columnQualifier, AccumuloNameSubstitutionStrategy nameSubstitutionStrategy) {
        this.parts = splitOnValueSeparator(columnQualifier);
        if (this.parts.length != 2) {
            throw new VertexiumException("Invalid property column qualifier: " + columnQualifier + ". Expected 2 parts, found " + this.parts.length);
        }
        parts[PART_INDEX_PROPERTY_NAME] = nameSubstitutionStrategy.inflate(parts[PART_INDEX_PROPERTY_NAME]);
        parts[PART_INDEX_PROPERTY_KEY] = nameSubstitutionStrategy.inflate(parts[PART_INDEX_PROPERTY_KEY]);
    }

    public PropertyColumnQualifier(Property property) {
        this.parts = new String[]{
                property.getName(),
                property.getKey()
        };
    }

    public PropertyColumnQualifier(String propertyName, String propertyKey) {
        this.parts = new String[]{
                propertyName,
                propertyKey
        };
    }

    public PropertyColumnQualifier(PropertyDeleteMutation propertyDeleteMutation) {
        this.parts = new String[]{
                propertyDeleteMutation.getName(),
                propertyDeleteMutation.getKey()
        };
    }

    public PropertyColumnQualifier(PropertySoftDeleteMutation propertySoftDeleteMutation) {
        this.parts = new String[]{
                propertySoftDeleteMutation.getName(),
                propertySoftDeleteMutation.getKey()
        };
    }

    public String getPropertyName() {
        return parts[PART_INDEX_PROPERTY_NAME];
    }

    public String getPropertyKey() {
        return parts[PART_INDEX_PROPERTY_KEY];
    }

    public String getDiscriminator(ColumnVisibility columnVisibility, long timestamp) {
        return getDiscriminator(AccumuloGraph.accumuloVisibilityToVisibility(columnVisibility), timestamp);
    }

    public String getDiscriminator(Visibility visibility, long timestamp) {
        return getDiscriminator(visibility.getVisibilityString(), timestamp);
    }

    public String getDiscriminator(String visibilityString, long timestamp) {
        assertNoValueSeparator(getPropertyName());
        assertNoValueSeparator(getPropertyKey());
        assertNoValueSeparator(visibilityString);
        return getPropertyName() + VALUE_SEPARATOR + getPropertyKey() + VALUE_SEPARATOR + visibilityString + VALUE_SEPARATOR + timestamp;
    }

    public Text getColumnQualifier(NameSubstitutionStrategy nameSubstitutionStrategy) {
        String name = nameSubstitutionStrategy.deflate(getPropertyName());
        String key = nameSubstitutionStrategy.deflate(getPropertyKey());
        assertNoValueSeparator(name);
        assertNoValueSeparator(key);
        return new Text(name + VALUE_SEPARATOR + key);
    }
}
