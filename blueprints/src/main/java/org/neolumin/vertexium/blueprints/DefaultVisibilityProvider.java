package org.neolumin.vertexium.blueprints;

import org.neolumin.vertexium.Vertex;
import org.neolumin.vertexium.Visibility;

import java.util.Map;

public class DefaultVisibilityProvider implements VisibilityProvider {
    private static final Visibility DEFAULT_VISIBILITY = new Visibility("");

    public DefaultVisibilityProvider(Map config) {

    }

    @Override
    public Visibility getVisibilityForEdge(String id, Vertex outVertex, Vertex inVertex, String label) {
        return DEFAULT_VISIBILITY;
    }

    @Override
    public Visibility getVisibilityForVertex(String id) {
        return DEFAULT_VISIBILITY;
    }

    @Override
    public Visibility getVisibilityForProperty(String propertyName, Object value) {
        return DEFAULT_VISIBILITY;
    }
}
