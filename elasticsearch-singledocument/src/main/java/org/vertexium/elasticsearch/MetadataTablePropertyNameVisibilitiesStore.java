package org.vertexium.elasticsearch;

import com.google.common.hash.Hashing;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.GraphMetadataEntry;
import org.vertexium.Visibility;

import java.nio.charset.Charset;
import java.util.*;

public class MetadataTablePropertyNameVisibilitiesStore extends PropertyNameVisibilitiesStore {
    public static final String METADATA_PREFIX = "propertyNameVisibility.";
    private static final Charset UTF8 = Charset.forName("utf8");
    private Map<String, Visibility> visibilityCache = new HashMap<>();

    public Collection<String> getHashes(Graph graph, String propertyName, Authorizations authorizations) {
        List<String> results = new ArrayList<>();
        String prefix = getMetadataPrefixWithPropertyName(propertyName);
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
        String metadataKey = getMetadataKey(propertyName, visibilityString);
        String hash = (String) graph.getMetadata(metadataKey);
        if (hash != null) {
            return hash;
        }

        hash = Hashing.murmur3_128().hashString(visibilityString, UTF8).toString();
        graph.setMetadata(metadataKey, hash);
        return hash;
    }

    private String getMetadataPrefixWithPropertyName(String propertyName) {
        return METADATA_PREFIX + propertyName + ".";
    }

    private String getMetadataKey(String propertyName, String visibilityString) {
        return getMetadataPrefixWithPropertyName(propertyName) + visibilityString;
    }
}
