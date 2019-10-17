package org.vertexium.accumulo.util;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

public class RangeUtils {
    public static Range createRangeFromString(String key) {
        return new Range(new Text(key));
    }
}
