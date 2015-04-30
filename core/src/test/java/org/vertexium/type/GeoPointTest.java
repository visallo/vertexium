package org.vertexium.type;

import org.junit.Test;
import org.vertexium.VertexiumException;

import static org.junit.Assert.assertEquals;

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
}