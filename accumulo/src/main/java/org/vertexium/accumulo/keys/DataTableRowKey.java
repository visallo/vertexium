package org.vertexium.accumulo.keys;

import org.apache.commons.lang.StringUtils;
import org.vertexium.Property;
import org.vertexium.accumulo.iterator.model.KeyBase;

public class DataTableRowKey extends KeyBase {
    private static final int LEGACY_VALUE_SEPARATOR_COUNT = 3;
    private static final int PARTS_INDEX_ELEMENT_ROW_KEY = 0;
    private static final int PARTS_INDEX_PROPERTY_NAME = 1;
    private static final int PARTS_INDEX_PROPERTY_KEY = 2;
    private final String[] parts;

    public DataTableRowKey(String elementRowKey, Property property) {
        this(elementRowKey, property.getKey(), property.getName());
    }

    public DataTableRowKey(String elementRowKey, String propertyKey, String propertyName) {
        this.parts = new String[]{
            elementRowKey,
            propertyName,
            propertyKey
        };
    }

    public String getRowKey() {
        assertNoValueSeparator(getElementRowKey());
        assertNoValueSeparator(getPropertyName());
        assertNoValueSeparator(getPropertyKey());
        return getElementRowKey()
            + VALUE_SEPARATOR + getPropertyName()
            + VALUE_SEPARATOR + getPropertyKey();
    }

    public String getElementRowKey() {
        return parts[PARTS_INDEX_ELEMENT_ROW_KEY];
    }

    public String getPropertyName() {
        return parts[PARTS_INDEX_PROPERTY_NAME];
    }

    public String getPropertyKey() {
        return parts[PARTS_INDEX_PROPERTY_KEY];
    }

    public static boolean isLegacy(String dataRowKey) {
        return StringUtils.countMatches(dataRowKey, "" + VALUE_SEPARATOR) == LEGACY_VALUE_SEPARATOR_COUNT;
    }
}
