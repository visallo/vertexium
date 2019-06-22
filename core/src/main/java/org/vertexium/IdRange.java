package org.vertexium;

public class IdRange extends Range<String> {
    /**
     * Creates a range object.
     *
     * @param inclusiveStart The inclusive start id. null if the start of all keys
     * @param exclusiveEnd   The exclusive end id. null if the end of all keys
     */
    public IdRange(String inclusiveStart, String exclusiveEnd) {
        super(inclusiveStart, true, exclusiveEnd, false);
    }

    /**
     * Creates a range object.
     *
     * @param start          The start value. null if the start of all.
     * @param inclusiveStart true, if the start value should be included.
     * @param end            The end value. null if the end of all.
     * @param inclusiveEnd   true, if the end value should be included.
     */
    public IdRange(String start, boolean inclusiveStart, String end, boolean inclusiveEnd) {
        super(start, inclusiveStart, end, inclusiveEnd);
    }
}
