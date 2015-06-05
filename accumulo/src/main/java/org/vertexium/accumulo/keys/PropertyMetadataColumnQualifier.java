package org.vertexium.accumulo.keys;

import org.apache.hadoop.io.Text;
import org.vertexium.Property;
import org.vertexium.VertexiumException;
import org.vertexium.id.NameSubstitutionStrategy;

public class PropertyMetadataColumnQualifier extends KeyBase {
    private static final int PART_INDEX_PROPERTY_NAME = 0;
    private static final int PART_INDEX_PROPERTY_KEY = 1;
    private static final int PART_INDEX_PROPERTY_VISIBILITY = 2;
    private static final int PART_INDEX_METADATA_KEY = 3;
    private final String[] parts;

    public PropertyMetadataColumnQualifier(Text columnQualifier, NameSubstitutionStrategy nameSubstitutionStrategy) {
        this.parts = splitOnValueSeparator(columnQualifier);
        if (this.parts.length != 4) {
            throw new VertexiumException("Invalid property metadata column qualifier: " + columnQualifier + ". Expected 4 parts, found " + this.parts.length);
        }
        parts[PART_INDEX_PROPERTY_NAME] = nameSubstitutionStrategy.inflate(parts[PART_INDEX_PROPERTY_NAME]);
        parts[PART_INDEX_PROPERTY_KEY] = nameSubstitutionStrategy.inflate(parts[PART_INDEX_PROPERTY_KEY]);
    }

    public PropertyMetadataColumnQualifier(Property property, String metadataKey) {
        parts = new String[]{
                property.getName(),
                property.getKey(),
                property.getVisibility().getVisibilityString(),
                metadataKey
        };
    }

    public PropertyColumnQualifier getPropertyColumnQualifier() {
        return new PropertyColumnQualifier(getPropertyName(), getPropertyKey());
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

    public String getMetadataKey() {
        return parts[PART_INDEX_METADATA_KEY];
    }

    public Text getColumnQualifier(NameSubstitutionStrategy nameSubstitutionStrategy) {
        String name = nameSubstitutionStrategy.deflate(getPropertyName());
        String key = nameSubstitutionStrategy.deflate(getPropertyKey());
        String visibilityString = getPropertyVisibilityString();
        String metadataKey = getMetadataKey();
        assertNoValueSeparator(name);
        assertNoValueSeparator(key);
        assertNoValueSeparator(visibilityString);
        assertNoValueSeparator(metadataKey);
        return new Text(new StringBuilder(name.length() + key.length() + visibilityString.length() + metadataKey.length() + 3)
                .append(name)
                .append(VALUE_SEPARATOR)
                .append(key)
                .append(VALUE_SEPARATOR)
                .append(visibilityString)
                .append(VALUE_SEPARATOR)
                .append(metadataKey).toString());
    }

    public String getPropertyDiscriminator(long propertyTimestamp) {
        return getPropertyColumnQualifier().getDiscriminator(getPropertyVisibilityString(), propertyTimestamp);
    }
}
