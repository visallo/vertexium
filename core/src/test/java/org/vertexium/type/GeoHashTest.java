package org.vertexium.type;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeoHashTest {
    @Test
    public void testCreate() {
        GeoHash hash = new GeoHash(48.669, -4.329, 5);
        assertEquals("gbsuv", hash.getHash());
        assertEquals(48.69140625, hash.toGeoRect().getNorthWest().getLatitude(), 0.001);
        assertEquals(-4.3505859375, hash.toGeoRect().getNorthWest().getLongitude(), 0.001);
        assertEquals(48.6474609375, hash.toGeoRect().getSouthEast().getLatitude(), 0.001);
        assertEquals(-4.306640625, hash.toGeoRect().getSouthEast().getLongitude(), 0.001);
        assertTrue(hash.toGeoRect().within(new GeoPoint(48.669, -4.329)));
    }
}