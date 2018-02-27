package org.vertexium.elasticsearch5.utils;

import org.junit.Test;

import javax.xml.bind.DatatypeConverter;

import static org.junit.Assert.assertEquals;

public class Murmur3Test {
    @Test
    public void testHash64() {
        // validated using http://murmurhash.shorelabs.com/  murmur3 x64 128bit seed 104729
        assertBytes("b3d427606b5971177385b6ff059f9904", Murmur3.hash128("This is a test".getBytes()));
        assertBytes("00a924bf9b531fe091fd4ddf4e0419c7", Murmur3.hash128("Some other test value".getBytes()));
    }

    private void assertBytes(String expectedHexString, byte[] found) {
        assertEquals(expectedHexString.toLowerCase(), DatatypeConverter.printHexBinary(found).toLowerCase());
    }
}