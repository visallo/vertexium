package org.vertexium;

import java.io.Serializable;

public class Range implements Serializable {
    private static final long serialVersionUID = -4491252292678133754L;
    private final String inclusiveStart;
    private final String exclusiveEnd;

    /**
     * Creates a range object.
     *
     * @param inclusiveStart The inclusive start id. null if the start of all keys
     * @param exclusiveEnd   The exclusive end id. null if the end of all keys
     */
    public Range(String inclusiveStart, String exclusiveEnd) {
        this.inclusiveStart = inclusiveStart;
        this.exclusiveEnd = exclusiveEnd;
    }

    public String getInclusiveStart() {
        return inclusiveStart;
    }

    public String getExclusiveEnd() {
        return exclusiveEnd;
    }

    public boolean isInRange(String str) {
        if (getInclusiveStart() != null) {
            if (getInclusiveStart().compareTo(str) > 0) {
                return false;
            }
        }

        if (getExclusiveEnd() != null) {
            if (str.compareTo(getExclusiveEnd()) >= 0) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        return "Range{" +
                "inclusiveStart='" + getInclusiveStart() + '\'' +
                ", exclusiveEnd='" + getExclusiveEnd() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Range range = (Range) o;

        if (inclusiveStart != null ? !inclusiveStart.equals(range.inclusiveStart) : range.inclusiveStart != null) {
            return false;
        }
        if (exclusiveEnd != null ? !exclusiveEnd.equals(range.exclusiveEnd) : range.exclusiveEnd != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = inclusiveStart != null ? inclusiveStart.hashCode() : 0;
        result = 31 * result + (exclusiveEnd != null ? exclusiveEnd.hashCode() : 0);
        return result;
    }
}
