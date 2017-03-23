package org.vertexium.elasticsearch2;

import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.Visibility;

import java.util.Collection;

public abstract class PropertyNameVisibilitiesStore {
    public abstract Collection<String> getHashes(Graph graph, String propertyName, Authorizations authorizations);

    public abstract String getHash(Graph graph, String propertyName, Visibility visibility);

    public abstract Visibility getVisibilityFromHash(Graph graph, String visibilityHash);
}
