package org.vertexium.accumulo.keys;

import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.model.KeyBase;
import org.vertexium.accumulo.iterator.model.PropertyColumnQualifier;
import org.vertexium.accumulo.iterator.model.PropertyMetadataColumnQualifier;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.mutation.ExtendedDataMutationBase;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

public class KeyHelper {
    private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY = ThreadLocal.withInitial(() -> Charset.forName("UTF-8").newEncoder().onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE));

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

    public static Text createExtendedDataRowKey(ExtendedDataRowId rowId) {
        return createExtendedDataRowKey(rowId.getElementType(), rowId.getElementId(), rowId.getTableName(), rowId.getRowId());
    }

    public static Text createExtendedDataRowKey(ElementType elementType, String elementId, String tableName, String row) {
        StringBuilder sb = new StringBuilder();
        if (elementType != null) {
            String elementTypePrefix = getExtendedDataRowKeyElementTypePrefix(elementType);
            sb.append(elementTypePrefix);
            if (elementId != null) {
                sb.append(elementId);
                if (tableName != null) {
                    sb.append(KeyBase.VALUE_SEPARATOR);
                    sb.append(tableName);
                    sb.append(KeyBase.VALUE_SEPARATOR);
                    if (row != null) {
                        sb.append(row);
                    }
                } else if (row != null) {
                    throw new VertexiumException("Cannot create partial key with missing inner value");
                }
            } else if (tableName != null || row != null) {
                throw new VertexiumException("Cannot create partial key with missing inner value");
            }
        } else if (elementId != null || tableName != null || row != null) {
            throw new VertexiumException("Cannot create partial key with missing inner value");
        }
        return new Text(sb.toString());
    }

    public static Text createExtendedDataColumnQualifier(ExtendedDataMutationBase edm) {
        if (edm.getKey() == null) {
            return new Text(edm.getColumnName());
        } else {
            return new Text(edm.getColumnName() + KeyBase.VALUE_SEPARATOR + edm.getKey());
        }
    }

    public static Range createExtendedDataRowKeyRange(ElementType elementType, Range elementIdRange) {
        String elementTypePrefix = getExtendedDataRowKeyElementTypePrefix(elementType);
        String inclusiveStart = elementIdRange.getInclusiveStart();
        if (inclusiveStart == null) {
            inclusiveStart = "";
        }
        inclusiveStart = elementTypePrefix + inclusiveStart;

        String exclusiveEnd = elementIdRange.getExclusiveEnd();
        if (exclusiveEnd != null) {
            exclusiveEnd = elementTypePrefix + exclusiveEnd;
        } else if (elementType == ElementType.EDGE) {
            exclusiveEnd = getExtendedDataRowKeyElementTypePrefix(ElementType.VERTEX);
        }
        return new Range(inclusiveStart, exclusiveEnd);
    }

    private static String getExtendedDataRowKeyElementTypePrefix(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return "V";
            case EDGE:
                return "E";
            default:
                throw new VertexiumException("Unhandled element type: " + elementType);
        }
    }

    public static ExtendedDataRowId parseExtendedDataRowId(Text accumuloRow) {
        return parseExtendedDataRowId(accumuloRow.toString());
    }

    public static ExtendedDataRowId parseExtendedDataRowId(String accumuloRow) {
        ElementType elementType = extendedDataRowIdElementTypePrefixToElementType(accumuloRow.charAt(0));
        String[] parts = accumuloRow.substring(1).split("" + KeyBase.VALUE_SEPARATOR);
        if (parts.length != 3) {
            throw new VertexiumException("Invalid Accumulo row key found for extended data, expected 3 parts found " + parts.length + ": " + accumuloRow);
        }
        return new ExtendedDataRowId(elementType, parts[0], parts[1], parts[2]);
    }

    private static ElementType extendedDataRowIdElementTypePrefixToElementType(char c) {
        switch (c) {
            case 'V':
                return ElementType.VERTEX;
            case 'E':
                return ElementType.EDGE;
            default:
                throw new VertexiumException("Invalid element type prefix: " + c);
        }
    }
}
