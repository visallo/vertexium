package org.vertexium.accumulo.keys;

import org.apache.hadoop.io.Text;
import org.vertexium.Property;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.AccumuloNameSubstitutionStrategy;
import org.vertexium.id.NameSubstitutionStrategy;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;

public class PropertyMetadataColumnQualifier extends KeyBase {
    private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY = new ThreadLocal() {
        protected CharsetEncoder initialValue() {
            return Charset.forName("UTF-8").newEncoder().onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
    };

    private static final int PART_INDEX_PROPERTY_NAME = 0;
    private static final int PART_INDEX_PROPERTY_KEY = 1;
    private static final int PART_INDEX_PROPERTY_VISIBILITY = 2;
    private static final int PART_INDEX_METADATA_KEY = 3;
    private final String[] parts;

    public PropertyMetadataColumnQualifier(Text columnQualifier, AccumuloNameSubstitutionStrategy nameSubstitutionStrategy) {
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

        int charCount = name.length() + key.length() + visibilityString.length() + metadataKey.length() + 3;
        CharBuffer qualifierChars = (CharBuffer)CharBuffer.allocate(charCount)
                .put(name).put(VALUE_SEPARATOR)
                .put(key).put(VALUE_SEPARATOR)
                .put(visibilityString).put(VALUE_SEPARATOR)
                .put(metadataKey).flip();

        CharsetEncoder encoder = ENCODER_FACTORY.get();
        encoder.reset();

        try {
            ByteBuffer encodedQualifier = encoder.encode(qualifierChars);
            Text result = new Text();
            result.set(encodedQualifier.array(), 0, encodedQualifier.limit());
            return result;
        } catch (CharacterCodingException cce) {
            throw new RuntimeException("This should never happen", cce);
        }
    }

    public String getPropertyDiscriminator(long propertyTimestamp) {
        return getPropertyColumnQualifier().getDiscriminator(getPropertyVisibilityString(), propertyTimestamp);
    }
}
