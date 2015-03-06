package org.neolumin.vertexium.cli.model;

import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.Graph;
import org.neolumin.vertexium.cli.VertexiumScript;

public abstract class ModelBase {
    public Graph getGraph() {
        return VertexiumScript.getGraph();
    }

    public Authorizations getAuthorizations() {
        return VertexiumScript.getAuthorizations();
    }
}
