package org.vertexium;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

public class FetchHints {
    private final boolean includeAllProperties;
    private final ImmutableSet<String> propertyNamesToInclude;
    private final boolean includeAllPropertyMetadata;
    private final ImmutableSet<String> metadataKeysToInclude;
    private final boolean includeHidden;
    private final boolean includeAllEdgeRefs;
    private final boolean includeOutEdgeRefs;
    private final boolean includeInEdgeRefs;
    private final ImmutableSet<String> edgeLabelsOfEdgeRefsToInclude;
    private final boolean includeEdgeLabelsAndCounts;
    private final boolean includeExtendedDataTableNames;

    public static final FetchHints NONE = new FetchHintsBuilder()
            .build();

    public static final FetchHints PROPERTIES_AND_METADATA = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .build();

    public static final FetchHints ALL = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeAllEdgeRefs(true)
            .setIncludeExtendedDataTableNames(true)
            .build();

    public static final FetchHints ALL_INCLUDING_HIDDEN = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeAllEdgeRefs(true)
            .setIncludeExtendedDataTableNames(true)
            .setIncludeHidden(true)
            .build();

    public static final FetchHints EDGE_REFS = new FetchHintsBuilder()
            .setIncludeAllEdgeRefs(true)
            .build();

    public static final FetchHints EDGE_LABELS = new FetchHintsBuilder()
            .setIncludeEdgeLabelsAndCounts(true)
            .build();

    FetchHints(
            boolean includeAllProperties,
            ImmutableSet<String> propertyNamesToInclude,
            boolean includeAllPropertyMetadata,
            ImmutableSet<String> metadataKeysToInclude,
            boolean includeHidden,
            boolean includeAllEdgeRefs,
            boolean includeOutEdgeRefs,
            boolean includeInEdgeRefs,
            ImmutableSet<String> edgeLabelsOfEdgeRefsToInclude,
            boolean includeEdgeLabelsAndCounts,
            boolean includeExtendedDataTableNames
    ) {
        this.includeAllProperties = includeAllProperties;
        this.propertyNamesToInclude = propertyNamesToInclude;
        this.includeAllPropertyMetadata = includeAllPropertyMetadata;
        this.metadataKeysToInclude = metadataKeysToInclude;
        this.includeHidden = includeHidden;
        this.includeAllEdgeRefs = includeAllEdgeRefs;
        this.includeOutEdgeRefs = includeOutEdgeRefs;
        this.includeInEdgeRefs = includeInEdgeRefs;
        this.edgeLabelsOfEdgeRefsToInclude = edgeLabelsOfEdgeRefsToInclude;
        this.includeEdgeLabelsAndCounts = includeEdgeLabelsAndCounts;
        this.includeExtendedDataTableNames = includeExtendedDataTableNames;
    }

    public boolean isIncludeAllProperties() {
        return includeAllProperties;
    }

    public ImmutableSet<String> getPropertyNamesToInclude() {
        return propertyNamesToInclude;
    }

    public boolean isIncludeAllPropertyMetadata() {
        return includeAllPropertyMetadata;
    }

    public ImmutableSet<String> getMetadataKeysToInclude() {
        return metadataKeysToInclude;
    }

    public boolean isIncludeHidden() {
        return includeHidden;
    }

    public boolean isIncludeAllEdgeRefs() {
        return includeAllEdgeRefs;
    }

    public boolean isIncludeOutEdgeRefs() {
        return includeOutEdgeRefs;
    }

    public boolean isIncludeInEdgeRefs() {
        return includeInEdgeRefs;
    }

    public ImmutableSet<String> getEdgeLabelsOfEdgeRefsToInclude() {
        return edgeLabelsOfEdgeRefsToInclude;
    }

    public boolean isIncludeEdgeLabelsAndCounts() {
        return includeEdgeLabelsAndCounts;
    }

    public boolean isIncludeExtendedDataTableNames() {
        return includeExtendedDataTableNames;
    }

    public boolean isIncludePropertyMetadata() {
        return isIncludeAllPropertyMetadata() || (getMetadataKeysToInclude() != null && getMetadataKeysToInclude().size() > 0);
    }

    public boolean isIncludeProperties() {
        return isIncludeAllProperties() || (getPropertyNamesToInclude() != null && getPropertyNamesToInclude().size() > 0);
    }

    public boolean isIncludePropertyAndMetadata(String propertyName) {
        return isIncludeProperty(propertyName) && isIncludeAllPropertyMetadata();
    }

    private boolean isIncludeProperty(String propertyName) {
        if (isIncludeAllProperties()) {
            return true;
        }
        if (getPropertyNamesToInclude() != null && getPropertyNamesToInclude().contains(propertyName)) {
            return true;
        }
        return false;
    }

    public boolean isIncludeEdgeRefs() {
        return isIncludeAllEdgeRefs() || isIncludeInEdgeRefs() || isIncludeOutEdgeRefs()
                || (getEdgeLabelsOfEdgeRefsToInclude() != null && getEdgeLabelsOfEdgeRefsToInclude().size() > 0);
    }

    public boolean hasEdgeLabelsOfEdgeRefsToInclude() {
        return getEdgeLabelsOfEdgeRefsToInclude() != null && getEdgeLabelsOfEdgeRefsToInclude().size() > 0;
    }

    @Override
    public String toString() {
        return "FetchHints{" +
                "includeAllProperties=" + includeAllProperties +
                ", propertyNamesToInclude=" + setToString(propertyNamesToInclude) +
                ", includeAllPropertyMetadata=" + includeAllPropertyMetadata +
                ", metadataKeysToInclude=" + setToString(metadataKeysToInclude) +
                ", includeHidden=" + includeHidden +
                ", includeAllEdgeRefs=" + includeAllEdgeRefs +
                ", edgeLabelsOfEdgeRefsToInclude=" + setToString(edgeLabelsOfEdgeRefsToInclude) +
                ", includeEdgeLabelsAndCounts=" + includeEdgeLabelsAndCounts +
                ", includeExtendedDataTableNames=" + includeExtendedDataTableNames +
                '}';
    }

    private String setToString(ImmutableSet<String> set) {
        return set == null ? "" : Joiner.on(",").join(set);
    }

    public static FetchHintsBuilder builder() {
        return new FetchHintsBuilder();
    }

    public static FetchHintsBuilder builder(FetchHints fetchHints) {
        return new FetchHintsBuilder(fetchHints);
    }
}
