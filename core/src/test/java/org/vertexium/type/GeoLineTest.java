package org.vertexium.type;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GeoLineTest {

    @Test
    public void testWithin() {
        GeoRect rect1 = new GeoRect(new GeoPoint(10, 0), new GeoPoint(0, 10));

        // Line within the square
        assertTrue(new GeoLine(new GeoPoint(1, 1), new GeoPoint(9, 9)).within(rect1));

        // line outside the square
        assertFalse(new GeoLine(new GeoPoint(11, 1), new GeoPoint(11, 9)).within(rect1));

        // line that cuts through the corner of the square
        assertFalse(new GeoLine(new GeoPoint(11, 8), new GeoPoint(8, 11)).within(rect1));
    }
}