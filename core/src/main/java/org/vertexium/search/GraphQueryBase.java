package org.vertexium.search;

import org.vertexium.Authorizations;
import org.vertexium.Graph;

public abstract class GraphQueryBase extends QueryBase implements GraphQuery {
    protected GraphQueryBase(Graph graph, String queryString, Authorizations authorizations) {
        super(graph, queryString, authorizations);
    }

    protected GraphQueryBase(Graph graph, String[] similarToFields, String similarToText, Authorizations authorizations) {
        super(graph, similarToFields, similarToText, authorizations);
    }
}
