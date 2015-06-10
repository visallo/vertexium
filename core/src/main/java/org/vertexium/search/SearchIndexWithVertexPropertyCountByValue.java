package org.vertexium.search;

import org.vertexium.Authorizations;

import java.util.Map;

public interface SearchIndexWithVertexPropertyCountByValue {
    Map<Object, Long> getVertexPropertyCountByValue(String propertyName, Authorizations authorizations);
}
