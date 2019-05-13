package org.vertexium.search;

import org.vertexium.Graph;
import org.vertexium.User;

public abstract class GraphQueryBase extends QueryBase implements GraphQuery {
    protected GraphQueryBase(Graph graph, String queryString, User user) {
        super(graph, queryString, user);
    }

    protected GraphQueryBase(Graph graph, String[] similarToFields, String similarToText, User user) {
        super(graph, similarToFields, similarToText, user);
    }
}
