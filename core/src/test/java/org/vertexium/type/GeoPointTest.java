package org.vertexium.type;

import org.junit.Test;
import org.vertexium.VertexiumException;

import static org.junit.Assert.*;

public class GeoPointTest {
    @Test
    public void testParse() throws Exception {
        assertEquals(new GeoPoint(38.9283, -77.1753), GeoPoint.parse("38.9283, -77.1753"));
        assertEquals(new GeoPoint(38.9283, -77.1753, 500.0), GeoPoint.parse("38.9283, -77.1753, 500"));
        assertEquals(new GeoPoint(38.9283, -77.1753), GeoPoint.parse("38° 55' 41.88\", -77° 10' 31.0794\""));

        try {
            GeoPoint.parse("38.9283");
            throw new RuntimeException("Expected an exception");
        } catch (VertexiumException ex) {
            // expected
        }

        try {
            GeoPoint.parse("38.9283, -77.1753, 500, 10");
            throw new RuntimeException("Expected an exception");
        } catch (VertexiumException ex) {
            // expected
        }
    }

    @Test
    public void testParseWithDescription() throws Exception {
        GeoPoint pt = GeoPoint.parse("Dulles International Airport, VA [38.9283, -77.1753]");
        assertEquals("Dulles International Airport, VA", pt.getDescription());
        assertEquals(38.9283, pt.getLatitude(), 0.001);
        assertEquals(-77.1753, pt.getLongitude(), 0.001);
    }

    @Test
    public void testDistanceFrom() {
        GeoPoint p1 = new GeoPoint(38.6270, -90.1994);
        GeoPoint p2 = new GeoPoint(39.0438, -77.4874);

        assertEquals(1101.13d, p1.distanceFrom(p2), 0.01d);
    }

    @Test
    public void testLongitudinalDistanceTo() {
        GeoPoint topLeft = new GeoPoint(10.0, 0.0);
        GeoPoint bottomRight = new GeoPoint(0.0, 10.0);
        assertEquals(-10.0, topLeft.longitudinalDistanceTo(bottomRight), 0.01);
        assertEquals(10.0, bottomRight.longitudinalDistanceTo(topLeft), 0.01);

        topLeft = new GeoPoint(10.0, -170);
        bottomRight = new GeoPoint(0.0, 170);
        assertEquals(-20.0, topLeft.longitudinalDistanceTo(bottomRight), 0.01);
        assertEquals(20.0, bottomRight.longitudinalDistanceTo(topLeft), 0.01);
    }

    @Test
    public void testIsSouthEastOf() {
        GeoPoint topLeft = new GeoPoint(10.0, 0.0);
        GeoPoint bottomRight = new GeoPoint(0.0, 10.0);
        assertFalse(topLeft.isSouthEastOf(bottomRight));
        assertTrue(bottomRight.isSouthEastOf(topLeft));

        topLeft = new GeoPoint(10.0, -170);
        bottomRight = new GeoPoint(0.0, 170);
        assertFalse(topLeft.isSouthEastOf(bottomRight));
        assertTrue(bottomRight.isSouthEastOf(topLeft));
    }

    @Test
    public void testIsNorthWestOf() {
        GeoPoint topLeft = new GeoPoint(10.0, 0.0);
        GeoPoint bottomRight = new GeoPoint(0.0, 10.0);
        assertTrue(topLeft.isNorthWestOf(bottomRight));
        assertFalse(bottomRight.isNorthWestOf(topLeft));

        topLeft = new GeoPoint(10.0, -170);
        bottomRight = new GeoPoint(0.0, 170);
        assertTrue(topLeft.isNorthWestOf(bottomRight));
        assertFalse(bottomRight.isNorthWestOf(topLeft));
    }
}