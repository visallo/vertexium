package org.vertexium.elasticsearch5;

import org.vertexium.GraphMetadataEntry;
import org.vertexium.User;
import org.vertexium.Visibility;
import org.vertexium.elasticsearch5.utils.PropertyNameUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.nio.charset.Charset;
import java.util.*;

public class PropertyNameVisibilitiesStore {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(PropertyNameVisibilitiesStore.class);
    public static final String PROPERTY_NAME_VISIBILITY_TO_HASH_PREFIX = "propertyNameVisibility.";
    public static final String HASH_TO_VISIBILITY = "visibilityHash.";
    private static final Charset UTF8 = Charset.forName("utf8");
    private final Elasticsearch5Graph graph;
    private Map<String, Visibility> visibilityCache = new HashMap<>();

    public PropertyNameVisibilitiesStore(Elasticsearch5Graph graph) {
        this.graph = graph;
    }

    public Collection<String> getHashesWithAuthorization(String authorization, User user) {
        List<String> hashes = new ArrayList<>();
        for (GraphMetadataEntry metadata : graph.getMetadataWithPrefix(HASH_TO_VISIBILITY)) {
            Visibility visibility = getVisibility((String) metadata.getValue());
            if (user.canRead(visibility) && visibility.hasAuthorization(authorization)) {
                String hash = metadata.getKey().substring(HASH_TO_VISIBILITY.length());
                hashes.add(hash);
            }
        }
        return hashes;
    }

    public Collection<String> getHashes(User user) {
        List<String> hashes = new ArrayList<>();
        for (GraphMetadataEntry metadata : graph.getMetadataWithPrefix(HASH_TO_VISIBILITY)) {
            Visibility visibility = getVisibility((String) metadata.getValue());
            if (user.canRead(visibility)) {
                String hash = metadata.getKey().substring(HASH_TO_VISIBILITY.length());
                hashes.add(hash);
            }
        }
        return hashes;
    }

    public Collection<String> getHashes(String propertyName, User user) {
        List<String> results = new ArrayList<>();
        String prefix = getPropertyNameVisibilityToHashPrefix(propertyName);
        for (GraphMetadataEntry metadata : graph.getMetadataWithPrefix(prefix)) {
            String visibilityString = metadata.getKey().substring(prefix.length());
            Visibility visibility = getVisibility(visibilityString);
            if (user.canRead(visibility)) {
                String hash = (String) metadata.getValue();
                results.add(hash);
            }
        }
        return results;
    }

    private Visibility getVisibility(String visibilityString) {
        return visibilityCache.computeIfAbsent(visibilityString, Visibility::new);
    }

    public String getHash(String propertyName, Visibility visibility) {
        String visibilityString = visibility.getVisibilityString();
        String propertyNameVisibilityToHashKey = getMetadataKey(propertyName, visibilityString);
        String hash = (String) graph.getMetadata(propertyNameVisibilityToHashKey);
        if (hash != null) {
            saveHashToVisibility(hash, visibilityString);
            return hash;
        }

        hash = PropertyNameUtils.getVisibilityHash(visibilityString);
        graph.setMetadata(propertyNameVisibilityToHashKey, hash);
        saveHashToVisibility(hash, visibilityString);
        return hash;
    }

    private void saveHashToVisibility(String hash, String visibilityString) {
        String hashToVisibilityKey = getHashToVisibilityKey(hash);
        String foundVisibilityString = (String) graph.getMetadata(hashToVisibilityKey);
        if (foundVisibilityString == null) {
            graph.setMetadata(hashToVisibilityKey, visibilityString);
        }
    }

    public Visibility getVisibilityFromHash(String visibilityHash) {
        String visibilityString = (String) graph.getMetadata(getHashToVisibilityKey(visibilityHash));
        if (visibilityString == null) {
            LOGGER.warn("Could not find visibility matching the hash \"%s\" in the metadata table.", visibilityHash);
            return null;
        }
        return new Visibility(visibilityString);
    }

    private String getHashToVisibilityKey(String visibilityHash) {
        return HASH_TO_VISIBILITY + visibilityHash;
    }

    private String getPropertyNameVisibilityToHashPrefix(String propertyName) {
        return PROPERTY_NAME_VISIBILITY_TO_HASH_PREFIX + propertyName + ".";
    }

    private String getMetadataKey(String propertyName, String visibilityString) {
        return getPropertyNameVisibilityToHashPrefix(propertyName) + visibilityString;
    }
}
