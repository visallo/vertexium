package org.vertexium.serializer.xstream;

import org.junit.jupiter.api.BeforeEach;
import org.vertexium.test.VertexiumSerializerTestBase;

public class XStreamVertexiumSerializerTest extends VertexiumSerializerTestBase {
    private XStreamVertexiumSerializer vertexiumSerializer;

    @BeforeEach
    public void before() {
        vertexiumSerializer = new XStreamVertexiumSerializer();
    }

    @Override
    protected byte[] getSerializableObjectBytes() {
        return new byte[]{
                60, 111, 114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117, 109, 46, 116,
                101, 115, 116, 46, 86, 101, 114, 116, 101, 120, 105, 117, 109, 83, 101, 114,
                105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 66, 97, 115, 101, 95,
                45, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 79, 98, 106,
                101, 99, 116, 115, 62, 10, 32, 32, 60, 115, 116, 97, 114, 116, 62, 83,
                84, 65, 82, 84, 60, 47, 115, 116, 97, 114, 116, 62, 10, 32, 32, 60,
                112, 114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117, 101, 62, 10, 32,
                32, 32, 32, 60, 115, 116, 111, 114, 101, 62, 116, 114, 117, 101, 60, 47,
                115, 116, 111, 114, 101, 62, 10, 32, 32, 32, 32, 60, 115, 101, 97, 114,
                99, 104, 73, 110, 100, 101, 120, 62, 116, 114, 117, 101, 60, 47, 115, 101,
                97, 114, 99, 104, 73, 110, 100, 101, 120, 62, 10, 32, 32, 60, 47, 112,
                114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117, 101, 62, 10, 32, 32,
                60, 115, 116, 114, 101, 97, 109, 105, 110, 103, 80, 114, 111, 112, 101, 114,
                116, 121, 86, 97, 108, 117, 101, 32, 99, 108, 97, 115, 115, 61, 34, 111,
                114, 103, 46, 118, 101, 114, 116, 101, 120, 105, 117, 109, 46, 112, 114, 111,
                112, 101, 114, 116, 121, 46, 68, 101, 102, 97, 117, 108, 116, 83, 116, 114,
                101, 97, 109, 105, 110, 103, 80, 114, 111, 112, 101, 114, 116, 121, 86, 97,
                108, 117, 101, 34, 62, 10, 32, 32, 32, 32, 60, 115, 116, 111, 114, 101,
                62, 116, 114, 117, 101, 60, 47, 115, 116, 111, 114, 101, 62, 10, 32, 32,
                32, 32, 60, 115, 101, 97, 114, 99, 104, 73, 110, 100, 101, 120, 62, 116,
                114, 117, 101, 60, 47, 115, 101, 97, 114, 99, 104, 73, 110, 100, 101, 120,
                62, 10, 32, 32, 32, 32, 60, 118, 97, 108, 117, 101, 84, 121, 112, 101,
                62, 91, 66, 60, 47, 118, 97, 108, 117, 101, 84, 121, 112, 101, 62, 10,
                32, 32, 32, 32, 60, 108, 101, 110, 103, 116, 104, 62, 52, 60, 47, 108,
                101, 110, 103, 116, 104, 62, 10, 32, 32, 60, 47, 115, 116, 114, 101, 97,
                109, 105, 110, 103, 80, 114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117,
                101, 62, 10, 32, 32, 60, 115, 116, 114, 101, 97, 109, 105, 110, 103, 80,
                114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117, 101, 82, 101, 102, 32,
                99, 108, 97, 115, 115, 61, 34, 111, 114, 103, 46, 118, 101, 114, 116, 101,
                120, 105, 117, 109, 46, 116, 101, 115, 116, 46, 86, 101, 114, 116, 101, 120,
                105, 117, 109, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115,
                116, 66, 97, 115, 101, 36, 84, 101, 115, 116, 83, 116, 114, 101, 97, 109,
                105, 110, 103, 80, 114, 111, 112, 101, 114, 116, 121, 86, 97, 108, 117, 101,
                82, 101, 102, 34, 62, 10, 32, 32, 32, 32, 60, 118, 97, 108, 117, 101,
                84, 121, 112, 101, 62, 91, 66, 60, 47, 118, 97, 108, 117, 101, 84, 121,
                112, 101, 62, 10, 32, 32, 32, 32, 60, 115, 101, 97, 114, 99, 104, 73,
                110, 100, 101, 120, 62, 116, 114, 117, 101, 60, 47, 115, 101, 97, 114, 99,
                104, 73, 110, 100, 101, 120, 62, 10, 32, 32, 60, 47, 115, 116, 114, 101,
                97, 109, 105, 110, 103, 80, 114, 111, 112, 101, 114, 116, 121, 86, 97, 108,
                117, 101, 82, 101, 102, 62, 10, 32, 32, 60, 101, 110, 100, 62, 69, 78,
                68, 60, 47, 101, 110, 100, 62, 10, 60, 47, 111, 114, 103, 46, 118, 101,
                114, 116, 101, 120, 105, 117, 109, 46, 116, 101, 115, 116, 46, 86, 101, 114,
                116, 101, 120, 105, 117, 109, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114,
                84, 101, 115, 116, 66, 97, 115, 101, 95, 45, 83, 101, 114, 105, 97, 108,
                105, 122, 97, 98, 108, 101, 79, 98, 106, 101, 99, 116, 115, 62
        };
    }

    @Override
    protected XStreamVertexiumSerializer getVertexiumSerializer() {
        return vertexiumSerializer;
    }
}