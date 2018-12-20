package org.vertexium.elasticsearch5.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Ascii85Test {
    @Test
    public void testEncode() {
        // validated using https://www.tools4noobs.com/online_tools/ascii85_encode/
        assertEquals("<+oue+DGm>@3BZ'F*%", Ascii85.encode("This is a test".getBytes()));
        assertEquals(";f?Ma+E)@8ATAo8ATMr9G%#30AH", Ascii85.encode("Some other test value".getBytes()));
    }

    @Test
    public void testDecode() {
        assertEquals("This is a test", new String(Ascii85.decode("<+oue+DGm>@3BZ'F*%")));
        assertEquals("Some other test value", new String(Ascii85.decode(";f?Ma+E)@8ATAo8ATMr9G%#30AH")));
    }
}