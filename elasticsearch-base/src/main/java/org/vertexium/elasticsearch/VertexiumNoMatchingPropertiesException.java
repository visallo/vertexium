package org.vertexium.elasticsearch;

import org.vertexium.VertexiumException;

public class VertexiumNoMatchingPropertiesException extends VertexiumException {
    public VertexiumNoMatchingPropertiesException(String propertyName) {
        super("Could not find matching property name: " + propertyName);
    }
}
