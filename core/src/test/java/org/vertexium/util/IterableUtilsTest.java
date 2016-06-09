package org.vertexium.util;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class IterableUtilsTest {

    @Test
    public void testIsEmpty() {
        assertTrue(IterableUtils.isEmpty(Collections.emptyList()));
        assertFalse(IterableUtils.isEmpty(Collections.singletonList("junit")));
    }
}
