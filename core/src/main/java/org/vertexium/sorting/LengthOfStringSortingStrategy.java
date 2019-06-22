package org.vertexium.sorting;

import org.vertexium.VertexiumObject;
import org.vertexium.query.SortDirection;

public class LengthOfStringSortingStrategy implements SortingStrategy {
    private final String propertyName;

    public LengthOfStringSortingStrategy(String propertyName) {
        this.propertyName = propertyName;
    }

    @Override
    public int compare(VertexiumObject o1, VertexiumObject o2, SortDirection direction) {
        int length1 = getLength(o1, direction);
        int length2 = getLength(o2, direction);
        int result = Integer.compare(length1, length2);
        if (direction == SortDirection.DESCENDING) {
            result = -result;
        }
        return result;
    }

    private int getLength(VertexiumObject o, SortDirection direction) {
        int length = direction == SortDirection.ASCENDING ? Integer.MAX_VALUE : 0;
        for (Object propertyValue : o.getPropertyValues(propertyName)) {
            int valueLength = propertyValue.toString().length();
            if (direction == SortDirection.ASCENDING) {
                length = Math.min(valueLength, length);
            } else {
                length = Math.max(valueLength, length);
            }
        }
        return length;
    }

    public String getPropertyName() {
        return propertyName;
    }
}
