package org.vertexium.elasticsearch;

import com.google.common.hash.Hashing;
import org.cache2k.Cache;
import org.cache2k.CacheBuilder;
import org.cache2k.CacheSource;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.GraphMetadataEntry;
import org.vertexium.Visibility;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MetadataTablePropertyNameVisibilitiesStore extends PropertyNameVisibilitiesStore {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(MetadataTablePropertyNameVisibilitiesStore.class);
    public static final String METADATA_PREFIX = "propertyNameVisibility.";
    private static final Charset UTF8 = Charset.forName("utf8");

    private final Cache<String, Hashes> hashesCache;
    private final Map<String, Visibility> visibilityCache = new HashMap<>();

    public MetadataTablePropertyNameVisibilitiesStore(final Graph graph) {
        this.hashesCache = CacheBuilder
                .newCache(String.class, Hashes.class)
                .name(MetadataTablePropertyNameVisibilitiesStore.class, "hashesCache-" + System.identityHashCode(this))
                .maxSize(100000)
                .eternal(true)
                .source(new CacheSource<String, Hashes>() {
                    @Override
                    public Hashes get(String propertyName) throws Throwable {
                        LOGGER.debug("cache miss for property: %s", propertyName);
                        Hashes hashes = new Hashes();
                        String prefix = getMetadataPrefixWithPropertyName(propertyName);
                        for (GraphMetadataEntry metadata : graph.getMetadataWithPrefix(prefix)) {
                            String visibilityString = metadata.getKey().substring(prefix.length());
                            Visibility visibility = getVisibility(visibilityString);
                            hashes.add(visibility, (String) metadata.getValue());
                        }
                        return hashes;
                    }
                })
                .build();
    }

    public Collection<String> getHashes(Graph graph, String propertyName, Authorizations authorizations) {
        Hashes hashes = getHashes(graph, propertyName);
        return hashes.get(authorizations);
    }

    public String getHash(Graph graph, String propertyName, Visibility visibility) {
        Hashes hashes = getHashes(graph, propertyName);
        String hash = hashes.get(visibility);
        if (hash != null) {
            return hash;
        }
        String visibilityString = visibility.getVisibilityString();
        String metadataKey = getMetadataKey(propertyName, visibilityString);
        hash = Hashing.murmur3_128().hashString(visibilityString, UTF8).toString();
        graph.setMetadata(metadataKey, hash);
        hashes.add(visibility, hash);
        onPropertyChanged(propertyName, hashes);
        return hash;
    }

    @SuppressWarnings("unused")
    protected void onPropertyChanged(String propertyName, Hashes hashes) {
        // subclass can override to respond to a change
    }

    protected final void clearHashesCache() {
        hashesCache.clear();
    }

    private Hashes getHashes(Graph graph, String propertyName) {
        return this.hashesCache.get(propertyName);
    }

    private Visibility getVisibility(String visibilityString) {
        Visibility visibility = visibilityCache.get(visibilityString);
        if (visibility == null) {
            visibility = new Visibility(visibilityString);
            visibilityCache.put(visibilityString, visibility);
        }
        return visibility;
    }

    private String getMetadataPrefixWithPropertyName(String propertyName) {
        return METADATA_PREFIX + propertyName + ".";
    }

    private String getMetadataKey(String propertyName, String visibilityString) {
        return getMetadataPrefixWithPropertyName(propertyName) + visibilityString;
    }

    protected static class Hashes implements Serializable {
        private final Map<Visibility, String> hashes = new HashMap<>();

        public void add(Visibility visibility, String hash) {
            hashes.put(visibility, hash);
        }

        public Collection<String> get(Authorizations authorizations) {
            Collection<String> results = new ArrayList<>();
            for (Map.Entry<Visibility, String> vh : hashes.entrySet()) {
                if (authorizations.canRead(vh.getKey())) {
                    results.add(vh.getValue());
                }
            }
            return results;
        }

        public String get(Visibility visibility) {
            return hashes.get(visibility);
        }
    }
}
