package org.vertexium.cli.model;

import org.vertexium.Edge;
import org.vertexium.FetchHint;
import org.vertexium.Property;
import org.vertexium.Visibility;

public class LazyEdgeProperty extends LazyProperty {
    private final String edgeId;

    public LazyEdgeProperty(String edgeId, String key, String name, Visibility visibility) {
        super(key, name, visibility);
        this.edgeId = edgeId;
    }

    @Override
    protected String getToStringHeaderLine() {
        return "edge @|bold " + getEdgeId() + "|@ property";
    }

    @Override
    protected Property getP() {
        Edge edge = getGraph().getEdge(getEdgeId(), FetchHint.ALL, getTime(), getAuthorizations());
        if (edge == null) {
            return null;
        }
        return edge.getProperty(getKey(), getName(), getVisibility());
    }

    public String getEdgeId() {
        return edgeId;
    }
}
