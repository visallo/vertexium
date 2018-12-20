package org.vertexium.type;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GeoCircleTest {
    @Test
    public void testBoundingBox() throws Exception {
        GeoCircle geoCircle = new GeoCircle(38.6270, -90.1994, 500);
        GeoRect boundingBox = geoCircle.getBoundingBox();
        assertEquals(43.1236, boundingBox.getNorthWest().getLatitude(), 0.0001d);
        assertEquals(-95.9590, boundingBox.getNorthWest().getLongitude(), 0.0001d);
        assertEquals(34.1303, boundingBox.getSouthEast().getLatitude(), 0.0001d);
        assertEquals(-84.4397, boundingBox.getSouthEast().getLongitude(), 0.0001d);

        geoCircle = new GeoCircle(0, -179, 500);
        boundingBox = geoCircle.getBoundingBox();
        assertEquals(4.4966, boundingBox.getNorthWest().getLatitude(), 0.0001d);
        assertEquals(176.5033, boundingBox.getNorthWest().getLongitude(), 0.0001d);
        assertEquals(-4.4966, boundingBox.getSouthEast().getLatitude(), 0.0001d);
        assertEquals(-174.5033, boundingBox.getSouthEast().getLongitude(), 0.0001d);
    }
}
