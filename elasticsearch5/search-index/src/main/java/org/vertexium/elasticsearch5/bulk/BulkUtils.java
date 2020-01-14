package org.vertexium.elasticsearch5.bulk;

import org.vertexium.ElementId;
import org.vertexium.ExtendedDataRowId;
import org.vertexium.VertexiumException;
import org.vertexium.VertexiumObjectId;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class BulkUtils {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(BulkUtils.class);

    public static int calculateSizeOfId(VertexiumObjectId vertexiumObjectId) {
        if (vertexiumObjectId instanceof ElementId) {
            return ((ElementId) vertexiumObjectId).getId().length();
        } else if (vertexiumObjectId instanceof ExtendedDataRowId) {
            ExtendedDataRowId extendedDataRowId = (ExtendedDataRowId) vertexiumObjectId;
            return extendedDataRowId.getElementId().length()
                + extendedDataRowId.getTableName().length()
                + extendedDataRowId.getRowId().length();
        } else {
            throw new VertexiumException("Unhandled VertexiumObjectId: " + vertexiumObjectId.getClass().getName());
        }
    }

    public static int calculateSizeOfMap(Map<?, ?> map) {
        int size = 0;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            size += calculateSizeOfValue(entry.getKey()) + calculateSizeOfValue(entry.getValue());
        }
        return size;
    }

    public static int calculateSizeOfCollection(Collection<?> list) {
        int size = 0;
        for (Object o : list) {
            size += calculateSizeOfValue(o);
        }
        return size;
    }

    public static int calculateSizeOfValue(Object value) {
        if (value instanceof String) {
            return ((String) value).length();
        } else if (value instanceof Boolean) {
            return 4;
        } else if (value instanceof Number || value instanceof Date) {
            return 8;
        } else if (value instanceof Collection) {
            return calculateSizeOfCollection((Collection<?>) value);
        } else if (value instanceof Map) {
            return calculateSizeOfMap((Map<?, ?>) value);
        } else {
            LOGGER.warn("unhandled object to calculate size for: " + value.getClass().getName() + ", defaulting to 100");
            return 100;
        }
    }
}
