package org.vertexium.accumulo.keys;

import org.apache.hadoop.io.Text;
import org.vertexium.VertexiumException;

import java.util.ArrayList;

public abstract class KeyBase {
    public static final char VALUE_SEPARATOR = '\u001f';

    protected String[] splitOnValueSeparator(Text v) {
        String s = v.toString();
        ArrayList<String> results = new ArrayList<>(10);
        int last = 0;
        int i = s.indexOf(VALUE_SEPARATOR);
        while (true) {
            if (i > 0) {
                results.add(s.substring(last, i));
                last = i + 1;
                i = s.indexOf(VALUE_SEPARATOR, last);
            } else {
                results.add(s.substring(last));
                break;
            }
        }
        return results.toArray(new String[results.size()]);
    }

    protected void assertNoValueSeparator(String str) {
        if (str.indexOf(VALUE_SEPARATOR) >= 0) {
            throw new VertexiumException("String cannot contain '" + VALUE_SEPARATOR + "' (0x1f): " + str);
        }
    }
}
