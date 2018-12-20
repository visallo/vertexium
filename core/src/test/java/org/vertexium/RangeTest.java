package org.vertexium;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangeTest {
    @Test
    public void testIsInRange() throws Exception {
        Range range = new Range("b", "c");
        assertTrue(range.isInRange("b"));
        assertTrue(range.isInRange("ba"));
        assertFalse(range.isInRange("c"));

        range = new Range(null, "c");
        assertTrue(range.isInRange("b"));
        assertFalse(range.isInRange("c"));

        range = new Range("b", null);
        assertFalse(range.isInRange("az"));
        assertTrue(range.isInRange("b"));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testHashCode() {
        Range range = new Range(null, null);
        range.hashCode();

        range = new Range("a", null);
        range.hashCode();

        range = new Range(null, "a");
        range.hashCode();

        range = new Range("a", "a");
        range.hashCode();
    }

    @Test
    public void testEquals() {
        assertTrue(new Range(null, null).equals(new Range(null, null)));
        assertTrue(new Range("a", null).equals(new Range("a", null)));
        assertTrue(new Range(null, "a").equals(new Range(null, "a")));
        assertTrue(new Range("a", "b").equals(new Range("a", "b")));

        assertFalse(new Range(null, null).equals(new Range("a", "c")));
        assertFalse(new Range("a", null).equals(new Range(null, "a")));
        assertFalse(new Range("a", "b").equals(new Range("a", "c")));
    }
}