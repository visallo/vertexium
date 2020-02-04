package org.vertexium.type;

import org.junit.Test;
import org.vertexium.util.GeoUtils;

import java.util.Arrays;

import static org.junit.Assert.*;

public class GeoCircleTest {
    @Test
    public void testBoundingBox() {
        GeoCircle geoCircle = new GeoCircle(38.6270, -90.1994, 500);
        GeoRect boundingBox = (GeoRect) GeoUtils.getEnvelope(geoCircle);
        assertEquals(43.1236, boundingBox.getNorthWest().getLatitude(), 0.0001d);
        assertEquals(-95.9590, boundingBox.getNorthWest().getLongitude(), 0.0001d);
        assertEquals(34.1303, boundingBox.getSouthEast().getLatitude(), 0.0001d);
        assertEquals(-84.4397, boundingBox.getSouthEast().getLongitude(), 0.0001d);

        geoCircle = new GeoCircle(0, -179, 500);
        boundingBox = (GeoRect) GeoUtils.getEnvelope(geoCircle);
        assertEquals(4.4966, boundingBox.getNorthWest().getLatitude(), 0.0001d);
        assertEquals(176.5033, boundingBox.getNorthWest().getLongitude(), 0.0001d);
        assertEquals(-4.4966, boundingBox.getSouthEast().getLatitude(), 0.0001d);
        assertEquals(-174.5033, boundingBox.getSouthEast().getLongitude(), 0.0001d);
    }

    @Test
    public void testWithin() {
        GeoCircle geoCircle = new GeoCircle(5.0, 5.0, 500);

        assertTrue(new GeoRect(new GeoPoint(7, 2), new GeoPoint(2, 7)).within(geoCircle));
        assertFalse(new GeoRect(new GeoPoint(10, 0), new GeoPoint(0, 10)).within(geoCircle));

        assertTrue(new GeoCircle(5, 5, 400).within(geoCircle));
        assertFalse(new GeoCircle(5, 5, 600).within(geoCircle));
        assertTrue(new GeoCircle(3, 3, 100).within(geoCircle));
        assertFalse(new GeoCircle(3, 3, 300).within(geoCircle));

        assertTrue(new GeoLine(Arrays.asList(new GeoPoint(7, 2), new GeoPoint(2, 7))).within(geoCircle));
        assertFalse(new GeoLine(Arrays.asList(new GeoPoint(0, 2), new GeoPoint(2, 7))).within(geoCircle));
    }
}
