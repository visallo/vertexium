package org.vertexium.elasticsearch7.utils;

import org.vertexium.DateOnly;
import org.vertexium.VertexiumException;
import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoShape;
import org.vertexium.type.IpV4Address;

import java.util.Date;

public class ElasticsearchTypes {
    public static String fromJavaClass(Class clazz) {
        if (clazz == String.class) {
            return "text";
        } else if (clazz == IpV4Address.class) {
            return "ip";
        } else if (clazz == Float.class || clazz == Float.TYPE) {
            return "float";
        } else if (clazz == Double.class || clazz == Double.TYPE) {
            return "double";
        } else if (clazz == Byte.class || clazz == Byte.TYPE) {
            return "byte";
        } else if (clazz == Short.class || clazz == Short.TYPE) {
            return "short";
        } else if (clazz == Integer.class || clazz == Integer.TYPE) {
            return "integer";
        } else if (clazz == Long.class || clazz == Long.TYPE) {
            return "long";
        } else if (clazz == Date.class || clazz == DateOnly.class) {
            return "date";
        } else if (clazz == Boolean.class || clazz == Boolean.TYPE) {
            return "boolean";
        } else if (clazz == GeoPoint.class) {
            return "geo_point";
        } else if (GeoShape.class.isAssignableFrom(clazz)) {
            return "geo_shape";
        } else if (Number.class.isAssignableFrom(clazz)) {
            return "double";
        } else {
            throw new VertexiumException("Unexpected value type for property: " + clazz.getName());
        }
    }
}
