package org.vertexium.accumulo.keys;

import org.apache.hadoop.io.Text;
import org.vertexium.VertexiumException;

public abstract class KeyBase {
    public static final char VALUE_SEPARATOR = '\u001f';

    protected String[] splitOnValueSeparator(Text v, int partCount) {
        String s = v.toString();
        String[] results = new String[partCount];
        int last = 0;
        int i = s.indexOf(VALUE_SEPARATOR);
        int partIndex = 0;
        while (true) {
            if (i > 0) {
                results[partIndex++] = s.substring(last, i);
                if (partIndex >= partCount) {
                    throw new VertexiumException("Invalid number of parts for '" + s + "'. Expected " + partCount + " found " + partIndex);
                }
                last = i + 1;
                i = s.indexOf(VALUE_SEPARATOR, last);
            } else {
                results[partIndex++] = s.substring(last);
                break;
            }
        }
        if (partIndex != partCount) {
            throw new VertexiumException("Invalid number of parts for '" + s + "'. Expected " + partCount + " found " + partIndex);
        }
        return results;
    }

    protected void assertNoValueSeparator(String str) {
        if (str.indexOf(VALUE_SEPARATOR) >= 0) {
            throw new VertexiumException("String cannot contain '" + VALUE_SEPARATOR + "' (0x1f): " + str);
        }
    }
}
