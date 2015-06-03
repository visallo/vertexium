package org.vertexium.type;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GeoRectTest {
    @Test
    public void testIntersects() throws Exception {
        GeoRect rect1 = new GeoRect(new GeoPoint(10, 0), new GeoPoint(0, 10));
        GeoRect rect2 = new GeoRect(new GeoPoint(5, 0), new GeoPoint(0, 5));
        assertTrue(rect1.intersects(rect2));
        assertTrue(rect2.intersects(rect1));

        // rect2 is outside and above rect1
        rect1 = new GeoRect(new GeoPoint(10, 0), new GeoPoint(0, 10));
        rect2 = new GeoRect(new GeoPoint(15, 0), new GeoPoint(11, 5));
        assertFalse(rect1.intersects(rect2));
        assertFalse(rect2.intersects(rect1));

        // rect2 is outside and to the left of rect1
        rect1 = new GeoRect(new GeoPoint(10, 0), new GeoPoint(0, 10));
        rect2 = new GeoRect(new GeoPoint(2, -4), new GeoPoint(-2, -2));
        assertFalse(rect1.intersects(rect2));
        assertFalse(rect2.intersects(rect1));
    }
}
