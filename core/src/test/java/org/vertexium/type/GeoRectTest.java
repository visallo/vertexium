package org.vertexium.type;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GeoRectTest {
    @Test
    public void testIntersects() {
        GeoRect rect1 = new GeoRect(new GeoPoint(10, 0), new GeoPoint(0, 10));
        GeoRect rect2 = new GeoRect(new GeoPoint(5, 0), new GeoPoint(0, 5));
        assertTrue(rect1.intersects(rect2));
        assertTrue(rect2.intersects(rect1));

        // rect2 is outside and above rect1
        rect2 = new GeoRect(new GeoPoint(15, 0), new GeoPoint(11, 5));
        assertFalse(rect1.intersects(rect2));
        assertFalse(rect2.intersects(rect1));

        // rect2 is outside and to the left of rect1
        rect2 = new GeoRect(new GeoPoint(2, -4), new GeoPoint(-2, -2));
        assertFalse(rect1.intersects(rect2));
        assertFalse(rect2.intersects(rect1));

        // Test intersecting with a point
        assertTrue(rect1.intersects(new GeoPoint(5, 5)));
        assertFalse(rect1.intersects(new GeoPoint(11, 11)));

        // Test intersecting with a line
        assertTrue(rect1.intersects(new GeoLine(new GeoPoint(1, 1), new GeoPoint(9, 9))));
        assertTrue(rect1.intersects(new GeoLine(new GeoPoint(-1, -1), new GeoPoint(11, 11))));
        assertFalse(rect1.intersects(new GeoLine(new GeoPoint(11, 11), new GeoPoint(12, 12))));
    }

    @Test
    public void testWithin() {
        GeoRect rect1 = new GeoRect(new GeoPoint(10, 0), new GeoPoint(0, 10));
        GeoRect rect2 = new GeoRect(new GeoPoint(5, 1), new GeoPoint(1, 5));
        assertFalse(rect1.within(rect2));
        assertTrue(rect2.within(rect1));

        // rect2 is outside and above rect1
        rect2 = new GeoRect(new GeoPoint(15, 0), new GeoPoint(11, 5));
        assertFalse(rect1.within(rect2));
        assertFalse(rect2.within(rect1));

        // rect2 is outside and to the left of rect1
        rect2 = new GeoRect(new GeoPoint(2, -4), new GeoPoint(-2, -2));
        assertFalse(rect1.within(rect2));
        assertFalse(rect2.within(rect1));

        // Test with a point
        assertFalse(rect1.within(new GeoPoint(5, 5)));
        assertFalse(rect1.within(new GeoPoint(11, 11)));

        // Test with a line
        assertFalse(rect1.within(new GeoLine(new GeoPoint(1, 1), new GeoPoint(9, 9))));
        assertFalse(rect1.within(new GeoLine(new GeoPoint(-1, -1), new GeoPoint(11, 11))));
        assertFalse(rect1.within(new GeoLine(new GeoPoint(11, 11), new GeoPoint(12, 12))));
    }
}
