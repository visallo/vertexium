package org.vertexium.cypher.utils;

import org.vertexium.VertexiumException;
import org.vertexium.cypher.ast.model.CypherMapLiteral;

import java.util.Map;

public class MapUtils {
    public static Object getByExpression(Map map, String property) {
        String[] path = property.split("\\.");
        Object value = null;
        for (String key : path) {
            if (map == null) {
                throw new VertexiumException("cannot get nested item from map: " + property);
            }
            value = map.get(key);
            if (value instanceof CypherMapLiteral) {
                map = ((CypherMapLiteral) value).toMap();
            } else if (value instanceof Map) {
                map = (Map) value;
            } else {
                map = null;
            }
        }
        return value;
    }
}
