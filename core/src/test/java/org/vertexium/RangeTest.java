package org.vertexium;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RangeTest {
    @Test
    public void testIsInRange() throws Exception {
        IdRange range = new IdRange("b", "c");
        assertTrue(range.isInRange("b"));
        assertTrue(range.isInRange("ba"));
        assertFalse(range.isInRange("c"));

        range = new IdRange(null, "c");
        assertTrue(range.isInRange("b"));
        assertFalse(range.isInRange("c"));

        range = new IdRange("b", null);
        assertFalse(range.isInRange("az"));
        assertTrue(range.isInRange("b"));

        range = new IdRange("b", true, "c", true);
        assertTrue(range.isInRange("b"));
        assertTrue(range.isInRange("ba"));
        assertTrue(range.isInRange("c"));

        range = new IdRange("b", false, "c", false);
        assertFalse(range.isInRange("b"));
        assertTrue(range.isInRange("ba"));
        assertFalse(range.isInRange("c"));

        range = new IdRange("b", true, "c", false);
        assertTrue(range.isInRange("b"));
        assertTrue(range.isInRange("ba"));
        assertFalse(range.isInRange("c"));

        range = new IdRange("b", false, "c", true);
        assertFalse(range.isInRange("b"));
        assertTrue(range.isInRange("ba"));
        assertTrue(range.isInRange("c"));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testHashCode() {
        IdRange range = new IdRange(null, null);
        range.hashCode();

        range = new IdRange("a", null);
        range.hashCode();

        range = new IdRange(null, "a");
        range.hashCode();

        range = new IdRange("a", "a");
        range.hashCode();
    }

    @Test
    public void testEquals() {
        assertTrue(new IdRange(null, null).equals(new IdRange(null, null)));
        assertTrue(new IdRange("a", null).equals(new IdRange("a", null)));
        assertTrue(new IdRange(null, "a").equals(new IdRange(null, "a")));
        assertTrue(new IdRange("a", "b").equals(new IdRange("a", "b")));

        assertFalse(new IdRange(null, null).equals(new IdRange("a", "c")));
        assertFalse(new IdRange("a", null).equals(new IdRange(null, "a")));
        assertFalse(new IdRange("a", "b").equals(new IdRange("a", "c")));
    }
}