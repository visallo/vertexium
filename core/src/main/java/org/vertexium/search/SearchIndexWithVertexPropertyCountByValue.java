package org.vertexium.search;

import org.vertexium.Authorizations;
import org.vertexium.Graph;

import java.util.Map;

public interface SearchIndexWithVertexPropertyCountByValue {
    @Deprecated
    Map<Object, Long> getVertexPropertyCountByValue(Graph graph, String propertyName, Authorizations authorizations);
}
