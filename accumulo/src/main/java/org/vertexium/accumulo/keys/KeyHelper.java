package org.vertexium.accumulo.keys;

import org.apache.hadoop.io.Text;
import org.vertexium.Property;
import org.vertexium.accumulo.iterator.model.KeyBase;
import org.vertexium.accumulo.iterator.model.PropertyColumnQualifier;
import org.vertexium.accumulo.iterator.model.PropertyMetadataColumnQualifier;
import org.vertexium.id.NameSubstitutionStrategy;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

public class KeyHelper {
    private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY = new ThreadLocal<CharsetEncoder>() {
        protected CharsetEncoder initialValue() {
            return Charset.forName("UTF-8").newEncoder().onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
    };

    public static PropertyMetadataColumnQualifier createPropertyMetadataColumnQualifier(String columnQualifier, NameSubstitutionStrategy nameSubstitutionStrategy) {
        String[] parts = KeyBase.splitOnValueSeparator(columnQualifier, 4);
        String propertyName = nameSubstitutionStrategy.inflate(parts[PropertyMetadataColumnQualifier.PART_INDEX_PROPERTY_NAME]);
        String propertyKey = nameSubstitutionStrategy.inflate(parts[PropertyMetadataColumnQualifier.PART_INDEX_PROPERTY_KEY]);
        String visibilityString = parts[PropertyMetadataColumnQualifier.PART_INDEX_PROPERTY_VISIBILITY];
        String metadataKey = nameSubstitutionStrategy.inflate(parts[PropertyMetadataColumnQualifier.PART_INDEX_METADATA_KEY]);
        return new PropertyMetadataColumnQualifier(propertyName, propertyKey, visibilityString, metadataKey);
    }

    public static PropertyColumnQualifier createPropertyColumnQualifier(String columnQualifier, NameSubstitutionStrategy nameSubstitutionStrategy) {
        String[] parts = KeyBase.splitOnValueSeparator(columnQualifier, 2);
        String propertyName = nameSubstitutionStrategy.inflate(parts[PropertyColumnQualifier.PART_INDEX_PROPERTY_NAME]);
        String propertyKey = nameSubstitutionStrategy.inflate(parts[PropertyColumnQualifier.PART_INDEX_PROPERTY_KEY]);
        return new PropertyColumnQualifier(propertyName, propertyKey);
    }

    public static Text getColumnQualifierFromPropertyColumnQualifier(Property property, NameSubstitutionStrategy nameSubstitutionStrategy) {
        return getColumnQualifierFromPropertyColumnQualifier(property.getKey(), property.getName(), nameSubstitutionStrategy);
    }

    public static Text getColumnQualifierFromPropertyColumnQualifier(String propertyKey, String propertyName, NameSubstitutionStrategy nameSubstitutionStrategy) {
        String name = nameSubstitutionStrategy.deflate(propertyName);
        String key = nameSubstitutionStrategy.deflate(propertyKey);
        KeyBase.assertNoValueSeparator(name);
        KeyBase.assertNoValueSeparator(key);
        //noinspection StringBufferReplaceableByString
        return new Text(new StringBuilder(name.length() + 1 + key.length())
                .append(name)
                .append(KeyBase.VALUE_SEPARATOR)
                .append(key)
                .toString()
        );
    }

    public static Text getColumnQualifierFromPropertyHiddenColumnQualifier(Property property, NameSubstitutionStrategy nameSubstitutionStrategy) {
        return getColumnQualifierFromPropertyHiddenColumnQualifier(property.getKey(), property.getName(), property.getVisibility().getVisibilityString(), nameSubstitutionStrategy);
    }

    public static Text getColumnQualifierFromPropertyHiddenColumnQualifier(String propertyKey, String propertyName, String visibilityString, NameSubstitutionStrategy nameSubstitutionStrategy) {
        String name = nameSubstitutionStrategy.deflate(propertyName);
        String key = nameSubstitutionStrategy.deflate(propertyKey);
        KeyBase.assertNoValueSeparator(name);
        KeyBase.assertNoValueSeparator(key);
        //noinspection StringBufferReplaceableByString
        return new Text(new StringBuilder(name.length() + 1 + key.length() + 1 + visibilityString.length())
                .append(name)
                .append(KeyBase.VALUE_SEPARATOR)
                .append(key)
                .append(KeyBase.VALUE_SEPARATOR)
                .append(visibilityString)
                .toString()
        );
    }

    public static Text getColumnQualifierFromPropertyMetadataColumnQualifier(String propertyName, String propertyKey, String visibilityString, String metadataKey, NameSubstitutionStrategy nameSubstitutionStrategy) {
        String name = nameSubstitutionStrategy.deflate(propertyName);
        String key = nameSubstitutionStrategy.deflate(propertyKey);
        metadataKey = nameSubstitutionStrategy.deflate(metadataKey);
        KeyBase.assertNoValueSeparator(name);
        KeyBase.assertNoValueSeparator(key);
        KeyBase.assertNoValueSeparator(visibilityString);
        KeyBase.assertNoValueSeparator(metadataKey);

        int charCount = name.length() + key.length() + visibilityString.length() + metadataKey.length() + 3;
        CharBuffer qualifierChars = (CharBuffer) CharBuffer.allocate(charCount)
                .put(name).put(KeyBase.VALUE_SEPARATOR)
                .put(key).put(KeyBase.VALUE_SEPARATOR)
                .put(visibilityString).put(KeyBase.VALUE_SEPARATOR)
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
}
