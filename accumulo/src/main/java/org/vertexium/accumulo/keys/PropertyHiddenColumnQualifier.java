package org.vertexium.accumulo.keys;

import org.apache.hadoop.io.Text;
import org.vertexium.Property;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.AccumuloNameSubstitutionStrategy;
import org.vertexium.id.NameSubstitutionStrategy;

public class PropertyHiddenColumnQualifier extends KeyBase {
    private static final int PART_INDEX_PROPERTY_NAME = 0;
    private static final int PART_INDEX_PROPERTY_KEY = 1;
    private static final int PART_INDEX_PROPERTY_VISIBILITY = 2;
    private final String[] parts;

    public PropertyHiddenColumnQualifier(Text columnQualifier, AccumuloNameSubstitutionStrategy nameSubstitutionStrategy) {
        parts = splitOnValueSeparator(columnQualifier);
        if (this.parts.length != 3) {
            throw new VertexiumException("Invalid property hidden column qualifier: " + columnQualifier + ". Expected 3 parts, found " + this.parts.length);
        }
        parts[PART_INDEX_PROPERTY_NAME] = nameSubstitutionStrategy.inflate(parts[PART_INDEX_PROPERTY_NAME]);
        parts[PART_INDEX_PROPERTY_KEY] = nameSubstitutionStrategy.inflate(parts[PART_INDEX_PROPERTY_KEY]);
    }

    public PropertyHiddenColumnQualifier(Property property) {
        this.parts = new String[]{
                property.getName(),
                property.getKey(),
                property.getVisibility().getVisibilityString()
        };
    }

    public String getPropertyName() {
        return parts[PART_INDEX_PROPERTY_NAME];
    }

    public String getPropertyKey() {
        return parts[PART_INDEX_PROPERTY_KEY];
    }

    public String getPropertyVisibilityString() {
        return parts[PART_INDEX_PROPERTY_VISIBILITY];
    }

    public Text getColumnQualifier(AccumuloNameSubstitutionStrategy nameSubstitutionStrategy) {
        String name = nameSubstitutionStrategy.deflate(getPropertyName());
        String key = nameSubstitutionStrategy.deflate(getPropertyKey());
        String visibilityString = getPropertyVisibilityString();
        assertNoValueSeparator(name);
        assertNoValueSeparator(key);
        assertNoValueSeparator(visibilityString);
        return new Text(name + VALUE_SEPARATOR + key + VALUE_SEPARATOR + visibilityString);
    }
}
