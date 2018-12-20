package org.vertexium.util;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IterableUtilsTest {

    @Test
    public void testIsEmpty() {
        assertTrue(IterableUtils.isEmpty(Collections.emptyList()));
        assertFalse(IterableUtils.isEmpty(Collections.singletonList("junit")));
    }
}
