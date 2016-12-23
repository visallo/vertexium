package org.vertexium.elasticsearch2;

import org.vertexium.VertexiumException;

public class VertexiumNoMatchingPropertiesException extends VertexiumException {
    private String propertyName;

    public VertexiumNoMatchingPropertiesException(String propertyName) {
        super("Could not find matching property name: " + propertyName);
        this.propertyName = propertyName;
    }

    public String getPropertyName() {
        return propertyName;
    }
}
