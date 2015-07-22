package org.vertexium.search;

import org.vertexium.Authorizations;
import org.vertexium.Graph;

import java.util.Map;

public interface SearchIndexWithVertexPropertyCountByValue {
    Map<Object, Long> getVertexPropertyCountByValue(Graph graph, String propertyName, Authorizations authorizations);
}
