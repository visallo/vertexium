package org.vertexium;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class TextIndexHintTest {
    @Test
    public void testToBits() {
        assertEquals((byte) 0x00, TextIndexHint.toBits(TextIndexHint.NONE));
        assertEquals((byte) 0x01, TextIndexHint.toBits(TextIndexHint.FULL_TEXT));
        assertEquals((byte) 0x02, TextIndexHint.toBits(TextIndexHint.EXACT_MATCH));
        assertEquals((byte) 0x03, TextIndexHint.toBits(TextIndexHint.ALL));
    }

    @Test
    public void testToSets() {
        assertTrue(TextIndexHint.toSet((byte) 0x01).contains(TextIndexHint.FULL_TEXT));
        assertTrue(TextIndexHint.toSet((byte) 0x02).contains(TextIndexHint.EXACT_MATCH));
        assertTrue(TextIndexHint.toSet((byte) 0x03).contains(TextIndexHint.FULL_TEXT));
        assertTrue(TextIndexHint.toSet((byte) 0x03).contains(TextIndexHint.EXACT_MATCH));
    }
}
