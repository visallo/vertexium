package org.vertexium;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;

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
    private final boolean ignoreAdditionalVisibilities;
    private final boolean includePreviousMetadata;

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
        boolean includeExtendedDataTableNames,
        boolean ignoreAdditionalVisibilities,
        boolean includePreviousMetadata
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
        this.ignoreAdditionalVisibilities = ignoreAdditionalVisibilities;
        this.includePreviousMetadata = includePreviousMetadata;
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

    public boolean isIgnoreAdditionalVisibilities() {
        return ignoreAdditionalVisibilities;
    }

    public boolean isIncludePreviousMetadata() {
        return includePreviousMetadata;
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

    public boolean isIncludeProperty(String propertyName) {
        if (isIncludeAllProperties()) {
            return true;
        }
        if (getPropertyNamesToInclude() != null && getPropertyNamesToInclude().contains(propertyName)) {
            return true;
        }
        return false;
    }

    public boolean isIncludeMetadata(String metadataKey) {
        if (isIncludeAllPropertyMetadata()) {
            return true;
        }
        if (getMetadataKeysToInclude() != null && getMetadataKeysToInclude().contains(metadataKey)) {
            return true;
        }
        return false;
    }

    public boolean isIncludeEdgeRefLabel(String label) {
        if (isIncludeAllEdgeRefs()) {
            return true;
        }
        if (getEdgeLabelsOfEdgeRefsToInclude() != null) {
            if (getEdgeLabelsOfEdgeRefsToInclude().contains(label)) {
                return true;
            } else {
                return false;
            }
        }
        if (isIncludeOutEdgeRefs() || isIncludeInEdgeRefs()) {
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

    public void validateHasEdgeFetchHints(Direction direction, String... labels) {
        if (!isIncludeEdgeRefs()) {
            throw new VertexiumMissingFetchHintException(this, "edgeRefs");
        }
        switch (direction) {
            case OUT:
                if (!isIncludeOutEdgeRefs() && !hasEdgeLabelsOfEdgeRefsToInclude()) {
                    throw new VertexiumMissingFetchHintException(this, "outEdgeRefs or edgeLabels");
                }
                break;
            case IN:
                if (!isIncludeInEdgeRefs() && !hasEdgeLabelsOfEdgeRefsToInclude()) {
                    throw new VertexiumMissingFetchHintException(this, "inEdgeRefs or edgeLabels");
                }
                break;
        }

        if (labels != null
            && labels.length != 0
            && !isIncludeAllEdgeRefs()
            && !isIncludeInEdgeRefs()
            && !isIncludeOutEdgeRefs()
            && (getEdgeLabelsOfEdgeRefsToInclude() != null && getEdgeLabelsOfEdgeRefsToInclude().size() > 0)) {
            for (String label : labels) {
                if (!getEdgeLabelsOfEdgeRefsToInclude().contains(label)) {
                    throw new VertexiumMissingFetchHintException(this, "edgeLabel:" + label);
                }
            }
        }
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
            ", ignoreAdditionalVisibilities=" + ignoreAdditionalVisibilities +
            ", includePreviousMetadata=" + includePreviousMetadata +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FetchHints that = (FetchHints) o;
        return includeAllProperties == that.includeAllProperties
            && includeAllPropertyMetadata == that.includeAllPropertyMetadata
            && includeHidden == that.includeHidden
            && includeAllEdgeRefs == that.includeAllEdgeRefs
            && includeOutEdgeRefs == that.includeOutEdgeRefs
            && includeInEdgeRefs == that.includeInEdgeRefs
            && includeEdgeLabelsAndCounts == that.includeEdgeLabelsAndCounts
            && includeExtendedDataTableNames == that.includeExtendedDataTableNames
            && ignoreAdditionalVisibilities == that.ignoreAdditionalVisibilities
            && includePreviousMetadata == that.includePreviousMetadata
            && Objects.equals(propertyNamesToInclude, that.propertyNamesToInclude)
            && Objects.equals(metadataKeysToInclude, that.metadataKeysToInclude)
            && Objects.equals(edgeLabelsOfEdgeRefsToInclude, that.edgeLabelsOfEdgeRefsToInclude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            includeAllProperties,
            propertyNamesToInclude,
            includeAllPropertyMetadata,
            metadataKeysToInclude,
            includeHidden,
            includeAllEdgeRefs,
            includeOutEdgeRefs,
            includeInEdgeRefs,
            edgeLabelsOfEdgeRefsToInclude,
            includeEdgeLabelsAndCounts,
            includeExtendedDataTableNames,
            ignoreAdditionalVisibilities,
            includePreviousMetadata
        );
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

    public void assertPropertyIncluded(String name) {
        if (isIncludeProperty(name)) {
            return;
        }
        throw new VertexiumMissingFetchHintException(this, "property:" + name);
    }

    public void assertMetadataIncluded(String key) {
        if (isIncludeMetadata(key)) {
            return;
        }
        throw new VertexiumMissingFetchHintException(this, "metadata:" + key);
    }
}
