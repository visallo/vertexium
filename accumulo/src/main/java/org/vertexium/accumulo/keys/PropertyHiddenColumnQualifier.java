package org.vertexium.accumulo.keys;

import org.apache.hadoop.io.Text;
import org.vertexium.Property;
import org.vertexium.accumulo.AccumuloNameSubstitutionStrategy;

public class PropertyHiddenColumnQualifier extends KeyBase {
    private static final int PART_INDEX_PROPERTY_NAME = 0;
    private static final int PART_INDEX_PROPERTY_KEY = 1;
    private static final int PART_INDEX_PROPERTY_VISIBILITY = 2;
    private final String[] parts;

    public PropertyHiddenColumnQualifier(Text columnQualifier, AccumuloNameSubstitutionStrategy nameSubstitutionStrategy) {
        parts = splitOnValueSeparator(columnQualifier, 3);
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
        return new Text(new StringBuilder(name.length() + 1 + key.length() + 1 + visibilityString.length())
                .append(name)
                .append(VALUE_SEPARATOR)
                .append(key)
                .append(VALUE_SEPARATOR)
                .append(visibilityString)
                .toString()
        );
    }
}
