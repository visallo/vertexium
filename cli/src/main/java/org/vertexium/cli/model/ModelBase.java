package org.vertexium.cli.model;

import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.cli.VertexiumScript;

public abstract class ModelBase {
    public Graph getGraph() {
        return VertexiumScript.getGraph();
    }

    public static Authorizations getAuthorizations() {
        return VertexiumScript.getAuthorizations();
    }

    public static Long getTime() {
        return VertexiumScript.getTime();
    }
}
