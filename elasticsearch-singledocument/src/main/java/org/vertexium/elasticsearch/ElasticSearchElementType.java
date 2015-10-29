package org.vertexium.elasticsearch;

import org.vertexium.VertexiumException;

public enum ElasticSearchElementType {
    VERTEX("vertex"),
    EDGE("edge");

    private final String key;

    ElasticSearchElementType(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public static ElasticSearchElementType parse(String s) {
        if (s.equals(VERTEX.getKey())) {
            return VERTEX;
        } else if (s.equals(EDGE.getKey())) {
            return EDGE;
        }
        throw new VertexiumException("Could not parse element type: " + s);
    }
}
