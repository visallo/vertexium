package org.vertexium.elasticsearch7;

import com.google.common.hash.Hashing;
import org.vertexium.*;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataTablePropertyNameVisibilitiesStore extends PropertyNameVisibilitiesStore {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(MetadataTablePropertyNameVisibilitiesStore.class);
    public static final String PROPERTY_NAME_VISIBILITY_TO_HASH_PREFIX = "propertyNameVisibility.";
    public static final String HASH_TO_VISIBILITY = "visibilityHash.";
    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private Map<String, Visibility> visibilityCache = new ConcurrentHashMap<>();

    public Collection<String> getHashesWithAuthorization(Graph graph, String authorization, Authorizations authorizations) {
        List<String> hashes = new ArrayList<>();
        for (GraphMetadataEntry metadata : graph.getMetadataWithPrefix(HASH_TO_VISIBILITY)) {
            Visibility visibility = getVisibility((String) metadata.getValue());
            if (authorizations.canRead(visibility) && visibility.hasAuthorization(authorization)) {
                String hash = metadata.getKey().substring(HASH_TO_VISIBILITY.length());
                hashes.add(hash);
            }
        }
        return hashes;
    }

    public Collection<String> getHashes(Graph graph, Authorizations authorizations) {
        List<String> hashes = new ArrayList<>();
        for (GraphMetadataEntry metadata : graph.getMetadataWithPrefix(HASH_TO_VISIBILITY)) {
            Visibility visibility = getVisibility((String) metadata.getValue());
            if (authorizations.canRead(visibility)) {
                String hash = metadata.getKey().substring(HASH_TO_VISIBILITY.length());
                hashes.add(hash);
            }
        }
        return hashes;
    }

    public Collection<String> getHashes(Graph graph, String propertyName, Authorizations authorizations) {
        List<String> results = new ArrayList<>();
        String prefix = getPropertyNameVisibilityToHashPrefix(propertyName);
        for (GraphMetadataEntry metadata : graph.getMetadataWithPrefix(prefix)) {
            String visibilityString = metadata.getKey().substring(prefix.length());
            Visibility visibility = getVisibility(visibilityString);
            if (authorizations.canRead(visibility)) {
                String hash = (String) metadata.getValue();
                results.add(hash);
            }
        }
        return results;
    }

    private Visibility getVisibility(String visibilityString) {
        return visibilityCache.computeIfAbsent(visibilityString, Visibility::new);
    }

    @Override
    public String addPropertyNameVisibility(Graph graph, String propertyName, Visibility visibility) {
        return getHash(graph, propertyName, visibility);
    }

    @Override
    public String getHash(Graph graph, String propertyName, Visibility visibility) {
        String visibilityString = visibility.getVisibilityString();
        String propertyNameVisibilityToHashKey = getMetadataKey(propertyName, visibilityString);
        String hash = (String) graph.getMetadata(propertyNameVisibilityToHashKey);
        if (hash != null) {
            saveHashToVisibility(graph, hash, visibilityString);
            return hash;
        }

        hash = Hashing.murmur3_128().hashString(visibilityString, UTF8).toString();
        graph.setMetadata(propertyNameVisibilityToHashKey, hash);
        saveHashToVisibility(graph, hash, visibilityString);
        return hash;
    }

    private void saveHashToVisibility(Graph graph, String hash, String visibilityString) {
        String hashToVisibilityKey = getHashToVisibilityKey(hash);
        String foundVisibilityString = (String) graph.getMetadata(hashToVisibilityKey);
        if (foundVisibilityString == null) {
            graph.setMetadata(hashToVisibilityKey, visibilityString);
        }
    }

    @Override
    public Visibility getVisibilityFromHash(Graph graph, String visibilityHash) {
        String metadataKey = getHashToVisibilityKey(visibilityHash);
        String visibilityString = (String) graph.getMetadata(metadataKey);
        if (visibilityString == null) {
            graph.reloadMetadata();
            visibilityString = (String) graph.getMetadata(metadataKey);
            if (visibilityString == null) {
                LOGGER.warn("Could not find visibility matching the hash \"%s\" in the metadata table with key \"%s\".", visibilityHash, metadataKey);
                return null;
            }
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
