package org.vertexium.accumulo.iterator.util;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DataInputStreamUtilsTest {
    @Test
    public void testDecodeEndingInZeroLengthByteArray() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
        DataOutputStreamUtils.encodeByteArray(out, new byte[0]);

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        byte[] arr = DataInputStreamUtils.decodeByteArray(in);
        assertNotNull(arr);
        assertEquals(0, arr.length);
    }

    @Test
    public void testDecodeEndingInZeroLengthByteSequence() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
        DataOutputStreamUtils.encodeByteSequence(out, new ArrayByteSequence(new byte[0]));

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        ByteSequence arr = DataInputStreamUtils.decodeByteSequence(in);
        assertNotNull(arr);
        assertEquals(0, arr.length());
    }
}
