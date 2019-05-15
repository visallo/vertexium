package org.vertexium.elasticsearch5;

import org.vertexium.Graph;
import org.vertexium.User;
import org.vertexium.Visibility;

import java.util.Collection;

public abstract class PropertyNameVisibilitiesStore {
    public abstract Collection<String> getHashes(Graph graph, User user);

    public abstract Collection<String> getHashes(Graph graph, String propertyName, User user);

    public abstract String getHash(Graph graph, String propertyName, Visibility visibility);

    public abstract Visibility getVisibilityFromHash(Graph graph, String visibilityHash);

    public abstract Collection<String> getHashesWithAuthorization(Graph graph, String authorization, User user);
}
