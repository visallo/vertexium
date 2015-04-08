package org.vertexium.blueprints;

import org.vertexium.Vertex;
import org.vertexium.Visibility;

public interface VisibilityProvider {
    Visibility getVisibilityForEdge(String id, Vertex outVertex, Vertex inVertex, String label);

    Visibility getVisibilityForVertex(String id);

    Visibility getVisibilityForProperty(String propertyName, Object value);
}
