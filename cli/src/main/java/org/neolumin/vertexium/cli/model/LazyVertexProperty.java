package org.neolumin.vertexium.cli.model;

import org.neolumin.vertexium.Property;
import org.neolumin.vertexium.Vertex;
import org.neolumin.vertexium.Visibility;

public class LazyVertexProperty extends LazyProperty {
    private final String vertexId;

    public LazyVertexProperty(String vertexId, String key, String name, Visibility visibility) {
        super(key, name, visibility);
        this.vertexId = vertexId;
    }

    @Override
    protected String getToStringHeaderLine() {
        return "vertex @|bold " + getVertexId() + "|@ property";
    }

    @Override
    protected Property getP() {
        Vertex vertex = getGraph().getVertex(getVertexId(), getAuthorizations());
        if (vertex == null) {
            return null;
        }
        return vertex.getProperty(getKey(), getName(), getVisibility());
    }

    public String getVertexId() {
        return vertexId;
    }
}
