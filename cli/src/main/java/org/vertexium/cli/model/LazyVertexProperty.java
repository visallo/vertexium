package org.vertexium.cli.model;

import org.vertexium.FetchHint;
import org.vertexium.Property;
import org.vertexium.Vertex;
import org.vertexium.Visibility;

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
    protected Vertex getE() {
        return getGraph().getVertex(getVertexId(), FetchHint.ALL_INCLUDING_HIDDEN, getTime(), getAuthorizations());
    }

    @Override
    protected Property getP() {
        Vertex vertex = getE();
        if (vertex == null) {
            return null;
        }
        return vertex.getProperty(getKey(), getName(), getVisibility());
    }

    public String getVertexId() {
        return vertexId;
    }
}
