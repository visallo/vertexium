package org.vertexium;

import org.vertexium.util.ObjectUtils;

import java.util.Objects;

public class Range<T> {
    private final T start;
    private final boolean inclusiveStart;
    private final T end;
    private final boolean inclusiveEnd;

    /**
     * Creates a range object.
     *
     * @param start          The start value. null if the start of all.
     * @param inclusiveStart true, if the start value should be included.
     * @param end            The end value. null if the end of all.
     * @param inclusiveEnd   true, if the end value should be included.
     */
    public Range(T start, boolean inclusiveStart, T end, boolean inclusiveEnd) {
        this.start = start;
        this.inclusiveStart = inclusiveStart;
        this.end = end;
        this.inclusiveEnd = inclusiveEnd;
    }

    public T getStart() {
        return start;
    }

    public boolean isInclusiveStart() {
        return inclusiveStart;
    }

    public T getEnd() {
        return end;
    }

    public boolean isInclusiveEnd() {
        return inclusiveEnd;
    }

    public boolean isInRange(Object obj) {
        if (getStart() != null) {
            if (isInclusiveStart()) {
                if (ObjectUtils.compare(getStart(), obj) > 0) {
                    return false;
                }
            } else {
                if (ObjectUtils.compare(getStart(), obj) >= 0) {
                    return false;
                }
            }
        }

        if (getEnd() != null) {
            if (isInclusiveEnd()) {
                if (ObjectUtils.compare(obj, getEnd()) > 0) {
                    return false;
                }
            } else {
                if (ObjectUtils.compare(obj, getEnd()) >= 0) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public String toString() {
        return String.format(
            "%s{start=%s, inclusiveStart=%s, end=%s, inclusiveEnd=%s}",
            this.getClass().getSimpleName(),
            start,
            inclusiveStart,
            end,
            inclusiveEnd
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Range)) {
            return false;
        }

        Range range = (Range) o;

        return inclusiveStart == range.inclusiveStart
            && inclusiveEnd == range.inclusiveEnd
            && Objects.equals(start, range.start)
            && Objects.equals(end, range.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, inclusiveStart, end, inclusiveEnd);
    }
}
