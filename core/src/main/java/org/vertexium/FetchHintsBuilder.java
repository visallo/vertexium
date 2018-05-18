package org.vertexium;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FetchHintsBuilder {
    private boolean includeAllProperties;
    private Set<String> propertyNamesToInclude;
    private boolean includeAllPropertyMetadata;
    private Set<String> metadataKeysToInclude;
    private boolean includeHidden;
    private boolean includeAllEdgeRefs;
    private boolean includeOutEdgeRefs;
    private boolean includeInEdgeRefs;
    private Set<String> edgeLabelsOfEdgeRefsToInclude;
    private boolean includeEdgeLabelsAndCounts;
    private boolean includeExtendedDataTableNames;

    public FetchHintsBuilder() {

    }

    public FetchHintsBuilder(FetchHints fetchHints) {
        includeAllProperties = fetchHints.isIncludeAllProperties();
        propertyNamesToInclude = fetchHints.getPropertyNamesToInclude();
        includeAllPropertyMetadata = fetchHints.isIncludeAllPropertyMetadata();
        metadataKeysToInclude = fetchHints.getMetadataKeysToInclude();
        includeHidden = fetchHints.isIncludeHidden();
        includeAllEdgeRefs = fetchHints.isIncludeAllEdgeRefs();
        includeOutEdgeRefs = fetchHints.isIncludeOutEdgeRefs();
        includeInEdgeRefs = fetchHints.isIncludeInEdgeRefs();
        edgeLabelsOfEdgeRefsToInclude = fetchHints.getEdgeLabelsOfEdgeRefsToInclude();
        includeEdgeLabelsAndCounts = fetchHints.isIncludeEdgeLabelsAndCounts();
        includeExtendedDataTableNames = fetchHints.isIncludeExtendedDataTableNames();
    }

    public FetchHints build() {
        if (!isIncludeProperties() && isIncludePropertyMetadata()) {
            includeAllProperties = true;
        }
        return new FetchHints(
                includeAllProperties,
                propertyNamesToInclude == null ? null : ImmutableSet.copyOf(propertyNamesToInclude),
                includeAllPropertyMetadata,
                metadataKeysToInclude == null ? null : ImmutableSet.copyOf(metadataKeysToInclude),
                includeHidden,
                includeAllEdgeRefs,
                includeOutEdgeRefs || includeAllEdgeRefs,
                includeInEdgeRefs || includeAllEdgeRefs,
                edgeLabelsOfEdgeRefsToInclude == null ? null : ImmutableSet.copyOf(edgeLabelsOfEdgeRefsToInclude),
                includeEdgeLabelsAndCounts,
                includeExtendedDataTableNames
        );
    }

    public static FetchHintsBuilder parse(JSONObject fetchHintsJson) {
        if (fetchHintsJson == null) {
            fetchHintsJson = new JSONObject();
        }

        return new FetchHintsBuilder()
                .setIncludeAllProperties(fetchHintsJson.optBoolean("includeAllProperties", false))
                .setIncludeAllPropertyMetadata(fetchHintsJson.optBoolean("includeAllPropertyMetadata", false))
                .setIncludeHidden(fetchHintsJson.optBoolean("includeHidden", false))
                .setIncludeAllEdgeRefs(fetchHintsJson.optBoolean("includeAllEdgeRefs", false))
                .setIncludeOutEdgeRefs(fetchHintsJson.optBoolean("includeOutEdgeRefs", false))
                .setIncludeInEdgeRefs(fetchHintsJson.optBoolean("includeInEdgeRefs", false))
                .setIncludeEdgeLabelsAndCounts(fetchHintsJson.optBoolean("includeEdgeLabelsAndCounts", false))
                .setIncludeExtendedDataTableNames(fetchHintsJson.optBoolean("includeExtendedDataTableNames", false))
                .setPropertyNamesToInclude(fetchHintsJson.has("propertyNamesToInclude")
                        ? jsonArrayToSet(fetchHintsJson.getJSONArray("propertyNamesToInclude"))
                        : null)
                .setMetadataKeysToInclude(fetchHintsJson.has("metadataKeysToInclude")
                        ? jsonArrayToSet(fetchHintsJson.getJSONArray("metadataKeysToInclude"))
                        : null)
                .setEdgeLabelsOfEdgeRefsToInclude(fetchHintsJson.has("edgeLabelsOfEdgeRefsToInclude")
                        ? jsonArrayToSet(fetchHintsJson.getJSONArray("edgeLabelsOfEdgeRefsToInclude"))
                        : null);
    }

    private static Set<String> jsonArrayToSet(JSONArray jsonArray) {
        if (jsonArray == null) {
            return null;
        }

        Set<String> set = new HashSet<>();
        for (int i = 0;i < jsonArray.length(); i++) {
            set.add((String) jsonArray.get(i));
        }

        return set;
    }

    private boolean isIncludeProperties() {
        return includeAllProperties || (propertyNamesToInclude != null && propertyNamesToInclude.size() > 0);
    }

    private boolean isIncludePropertyMetadata() {
        return includeAllPropertyMetadata || (metadataKeysToInclude != null && metadataKeysToInclude.size() > 0);
    }

    public FetchHintsBuilder setIncludeAllProperties(boolean includeAllProperties) {
        this.includeAllProperties = includeAllProperties;
        return this;
    }

    public FetchHintsBuilder setPropertyNamesToInclude(Set<String> propertyNamesToInclude) {
        this.propertyNamesToInclude = propertyNamesToInclude;
        return this;
    }

    public FetchHintsBuilder setPropertyNamesToInclude(String... propertyNamesToInclude) {
        this.propertyNamesToInclude = Sets.newHashSet(propertyNamesToInclude);
        return this;
    }

    public FetchHintsBuilder setIncludeAllPropertyMetadata(boolean includeAllPropertyMetadata) {
        this.includeAllPropertyMetadata = includeAllPropertyMetadata;
        return this;
    }

    public FetchHintsBuilder setMetadataKeysToInclude(Set<String> metadataKeysToInclude) {
        this.metadataKeysToInclude = metadataKeysToInclude;
        return this;
    }

    public FetchHintsBuilder setMetadataKeysToInclude(String... metadataKeysToInclude) {
        this.metadataKeysToInclude = Sets.newHashSet(metadataKeysToInclude);
        return this;
    }

    public FetchHintsBuilder setIncludeHidden(boolean includeHidden) {
        this.includeHidden = includeHidden;
        return this;
    }

    public FetchHintsBuilder setIncludeAllEdgeRefs(boolean includeAllEdgeRefs) {
        this.includeAllEdgeRefs = includeAllEdgeRefs;
        return this;
    }

    public FetchHintsBuilder setIncludeOutEdgeRefs(boolean includeOutEdgeRefs) {
        this.includeOutEdgeRefs = includeOutEdgeRefs;
        return this;
    }

    public FetchHintsBuilder setIncludeInEdgeRefs(boolean includeInEdgeRefs) {
        this.includeInEdgeRefs = includeInEdgeRefs;
        return this;
    }

    public FetchHintsBuilder setEdgeLabelsOfEdgeRefsToInclude(Set<String> edgeLabelsOfEdgeRefsToInclude) {
        this.edgeLabelsOfEdgeRefsToInclude = edgeLabelsOfEdgeRefsToInclude;
        return this;
    }

    public FetchHintsBuilder setEdgeLabelsOfEdgeRefsToInclude(String... edgeLabelsOfEdgeRefsToInclude) {
        this.edgeLabelsOfEdgeRefsToInclude = Sets.newHashSet(edgeLabelsOfEdgeRefsToInclude);
        return this;
    }

    public FetchHintsBuilder setIncludeEdgeLabelsAndCounts(boolean includeEdgeLabelsAndCounts) {
        this.includeEdgeLabelsAndCounts = includeEdgeLabelsAndCounts;
        return this;
    }

    public FetchHintsBuilder setIncludeExtendedDataTableNames(boolean includeExtendedDataTableNames) {
        this.includeExtendedDataTableNames = includeExtendedDataTableNames;
        return this;
    }
}
