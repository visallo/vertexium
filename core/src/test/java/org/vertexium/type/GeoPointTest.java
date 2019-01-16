package org.vertexium.type;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.vertexium.VertexiumException;

import static org.junit.Assert.*;

public class GeoPointTest {
    @Test
    public void testParse() throws Exception {
        assertEquals(new GeoPoint(38.9283, -77.1753), GeoPoint.parse("38.9283, -77.1753"));
        assertEquals(new GeoPoint(38.9283, -77.1753, 500.0), GeoPoint.parse("38.9283, -77.1753, 500"));
        assertEquals(new GeoPoint(38.9283, -77.1753, 500.0, 25.0), GeoPoint.parse("38.9283, -77.1753, 500, ~25"));
        assertEquals(new GeoPoint(38.9283, -77.1753, null, 25.0), GeoPoint.parse("38.9283, -77.1753, ~25"));
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

        try {
            GeoPoint.parse("38.9283, -77.1753, ~500, ~10");
            throw new RuntimeException("Expected an exception");
        } catch (VertexiumException ex) {
            // expected
        }

        try {
            GeoPoint.parse("38.9283, -77.1753, 500, 10, 10");
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
    public void testDistanceFromOppositeSidesOfEarth() {
        GeoPoint p1 = new GeoPoint(0.0, 0.0);
        GeoPoint p2 = new GeoPoint(0.0, 180.0);
        assertEquals(GeoPoint.EARTH_CIRCUMFERENCE / 2.0, p1.distanceFrom(p2), 0.01d);
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
    public void testCalculateCenter1Points() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0, 100.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1));
        GeoPoint expected = new GeoPoint(10.0, 20.0, 100.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenter2Points() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0, 100.0);
        GeoPoint pt2 = new GeoPoint(10.1, 20.1, 200.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(10.05, 20.05, 150.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenter2PointsSameLongitude() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0, 100.0);
        GeoPoint pt2 = new GeoPoint(10.1, 20.0, 200.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(10.05, 20.0, 150.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenter2PointsSameLatitude() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0, 100.0);
        GeoPoint pt2 = new GeoPoint(10.0, 20.1, 200.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(10.0, 20.05, 150.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenter2PointsSamePoint() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0);
        GeoPoint pt2 = new GeoPoint(10.0, 20.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(10.0, 20.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenter4Points() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0);
        GeoPoint pt2 = new GeoPoint(20.0, 30.0);
        GeoPoint pt3 = new GeoPoint(0.0, 40.0);
        GeoPoint pt4 = new GeoPoint(40.0, 10.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(15.05467090, 24.88248913);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenterPointsAroundZero() {
        GeoPoint pt1 = new GeoPoint(10.0, 10.0);
        GeoPoint pt2 = new GeoPoint(350.0, 80.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(0.0, 45.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenterPointsOppositeSides() {
        GeoPoint pt1 = new GeoPoint(90.0, 0.0);
        GeoPoint pt2 = new GeoPoint(-90.0, 0.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(0.0, 0.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);

        pt1 = new GeoPoint(0.0, 0.0);
        pt2 = new GeoPoint(0.0, 180.0);
        center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        expected = new GeoPoint(0.0, 90.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenterWithMissingAltitude() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0, 10.0);
        GeoPoint pt2 = new GeoPoint(10.1, 20.1);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(10.05, 20.05);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
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