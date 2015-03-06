package org.neolumin.vertexium.query;

import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.Graph;
import org.neolumin.vertexium.PropertyDefinition;

import java.util.Map;

public abstract class GraphQueryBase extends QueryBase implements GraphQuery {
    protected GraphQueryBase(Graph graph, String queryString, Map<String, PropertyDefinition> propertyDefinitions, Authorizations authorizations) {
        super(graph, queryString, propertyDefinitions, authorizations);
    }

    protected GraphQueryBase(Graph graph, String[] similarToFields, String similarToText, Map<String, PropertyDefinition> propertyDefinitions, Authorizations authorizations) {
        super(graph, similarToFields, similarToText, propertyDefinitions, authorizations);
    }
}
