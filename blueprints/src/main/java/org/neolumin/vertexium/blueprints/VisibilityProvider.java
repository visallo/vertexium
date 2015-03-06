package org.neolumin.vertexium.blueprints;

import org.neolumin.vertexium.Vertex;
import org.neolumin.vertexium.Visibility;

public interface VisibilityProvider {
    Visibility getVisibilityForEdge(String id, Vertex outVertex, Vertex inVertex, String label);

    Visibility getVisibilityForVertex(String id);

    Visibility getVisibilityForProperty(String propertyName, Object value);
}
