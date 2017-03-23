package org.vertexium.elasticsearch2;

import com.google.common.hash.Hashing;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.GraphMetadataEntry;
import org.vertexium.Visibility;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.nio.charset.Charset;
import java.util.*;

public class MetadataTablePropertyNameVisibilitiesStore extends PropertyNameVisibilitiesStore {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(MetadataTablePropertyNameVisibilitiesStore.class);
    public static final String PROPERTY_NAME_VISIBILITY_TO_HASH_PREFIX = "propertyNameVisibility.";
    public static final String HASH_TO_VISIBILITY = "visibilityHash.";
    private static final Charset UTF8 = Charset.forName("utf8");
    private Map<String, Visibility> visibilityCache = new HashMap<>();

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
        Visibility visibility = visibilityCache.get(visibilityString);
        if (visibility == null) {
            visibility = new Visibility(visibilityString);
            visibilityCache.put(visibilityString, visibility);
        }
        return visibility;
    }

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
