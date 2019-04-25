package org.vertexium.util;

import org.junit.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class MultiFieldStringEncoderTest {
    @Test
    public void test() {
        MultiFieldStringEncoder encoder = new MultiFieldStringEncoder("&", 4);
        ZonedDateTime timestamp = ZonedDateTime.of(2019, 10, 12, 13, 14, 15, 0, ZoneOffset.UTC);
        String s = encoder.encode(MultiFieldStringEncoder.timestampToString(timestamp), "b\u001f", "c%1f", "d&");
        assertEquals("16dc01af458&b%1F&c%251f&d%26", s);
        String[] parts = encoder.decode(s);
        assertEquals(timestamp, MultiFieldStringEncoder.timestampFromString(parts[0], ZoneOffset.UTC));
        assertEquals("b\u001f", parts[1]);
        assertEquals("c%1f", parts[2]);
        assertEquals("d&", parts[3]);
    }
}