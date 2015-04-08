package org.vertexium.elasticsearch;

import org.elasticsearch.common.geo.GeoHashUtils;
import org.vertexium.VertexiumException;

import java.lang.reflect.Method;

class GeohashUtils {
    private static final Method decodeCell;

    static {
        try {
            decodeCell = GeoHashUtils.class.getDeclaredMethod("decodeCell", String.class);
        } catch (NoSuchMethodException e) {
            throw new VertexiumException("Could not find decodeCell method", e);
        }
    }

    public static void decodeCell(String key, org.elasticsearch.common.geo.GeoPoint northWest, org.elasticsearch.common.geo.GeoPoint southEast) {
        try {
            double[] interval = (double[]) decodeCell.invoke(null, key);
            northWest.reset(interval[1], interval[2]);
            southEast.reset(interval[0], interval[3]);
        } catch (Exception e) {
            throw new VertexiumException("Could not decode cell", e);
        }
    }
}
