package org.vertexium.elasticsearch;

import com.google.common.hash.Hashing;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.Visibility;

import java.nio.charset.Charset;
import java.util.*;

public class InMemoryPropertyNameVisibilitiesStore extends PropertyNameVisibilitiesStore {
    private static final Charset UTF8 = Charset.forName("utf8");
    private Map<String, PropertyNameVisibilities> propertyNameVisibilitiesMap = new HashMap<>();

    public Collection<String> getHashes(Graph graph, String propertyName, Authorizations authorizations) {
        PropertyNameVisibilities propertyNameVisibilities = this.propertyNameVisibilitiesMap.get(propertyName);
        if (propertyNameVisibilities == null) {
            throw new VertexiumNoMatchingPropertiesException(propertyName);
        }
        Collection<String> hashes = propertyNameVisibilities.getHashes(authorizations);
        if (hashes.size() == 0) {
            throw new VertexiumNoMatchingPropertiesException(propertyName);
        }
        return hashes;
    }

    public String getHash(Graph graph, String propertyName, Visibility visibility) {
        PropertyNameVisibilities propertyNameVisibilities = this.propertyNameVisibilitiesMap.get(propertyName);
        if (propertyNameVisibilities == null) {
            propertyNameVisibilities = new PropertyNameVisibilities();
            this.propertyNameVisibilitiesMap.put(propertyName, propertyNameVisibilities);
        }
        return propertyNameVisibilities.getHash(visibility);
    }

    private static class PropertyNameVisibilities {
        private Map<Visibility, String> visibilityToSuffixMap = new HashMap<>();

        public String getHash(Visibility visibility) {
            String suffix = visibilityToSuffixMap.get(visibility);
            if (suffix == null) {
                suffix = "_" + Hashing.murmur3_128().hashString(visibility.getVisibilityString(), UTF8).toString();
                visibilityToSuffixMap.put(visibility, suffix);
            }
            return suffix;
        }

        public Collection<String> getHashes(Authorizations authorizations) {
            List<String> suffixes = new ArrayList<>();
            for (Map.Entry<Visibility, String> e : visibilityToSuffixMap.entrySet()) {
                if (authorizations.canRead(e.getKey())) {
                    suffixes.add(e.getValue());
                }
            }
            return suffixes;
        }
    }
}
