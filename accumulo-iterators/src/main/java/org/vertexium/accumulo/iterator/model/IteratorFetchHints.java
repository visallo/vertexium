package org.vertexium.accumulo.iterator.model;

import org.apache.accumulo.core.data.ByteSequence;
import org.vertexium.accumulo.iterator.util.ByteSequenceUtils;

import java.io.Serializable;
import java.util.Set;
import java.util.stream.Collectors;

public class IteratorFetchHints implements Serializable {
    private static final long serialVersionUID = -6302969731435846529L;
    private final boolean includeAllProperties;
    private final Set<ByteSequence> propertyNamesToInclude;
    private final boolean includeAllPropertyMetadata;
    private final Set<ByteSequence> metadataKeysToInclude;
    private final boolean includeHidden;
    private final boolean includeAllEdgeRefs;
    private final boolean includeOutEdgeRefs;
    private final boolean includeInEdgeRefs;
    private final boolean ignoreAdditionalVisibilities;
    private final Set<String> edgeLabelsOfEdgeRefsToInclude;
    private final boolean includeEdgeLabelsAndCounts;
    private final boolean includeExtendedDataTableNames;

    public IteratorFetchHints() {
        this.includeAllProperties = false;
        this.propertyNamesToInclude = null;
        this.includeAllPropertyMetadata = false;
        this.metadataKeysToInclude = null;
        this.includeHidden = false;
        this.includeAllEdgeRefs = false;
        this.includeOutEdgeRefs = false;
        this.includeInEdgeRefs = false;
        this.ignoreAdditionalVisibilities = false;
        this.edgeLabelsOfEdgeRefsToInclude = null;
        this.includeEdgeLabelsAndCounts = false;
        this.includeExtendedDataTableNames = false;
    }

    public IteratorFetchHints(
        boolean includeAllProperties,
        Set<ByteSequence> propertyNamesToInclude,
        boolean includeAllPropertyMetadata,
        Set<ByteSequence> metadataKeysToInclude,
        boolean includeHidden,
        boolean includeAllEdgeRefs,
        boolean includeOutEdgeRefs,
        boolean includeInEdgeRefs,
        boolean ignoreAdditionalVisibilities,
        Set<String> edgeLabelsOfEdgeRefsToInclude,
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
        this.ignoreAdditionalVisibilities = ignoreAdditionalVisibilities;
        this.edgeLabelsOfEdgeRefsToInclude = edgeLabelsOfEdgeRefsToInclude;
        this.includeEdgeLabelsAndCounts = includeEdgeLabelsAndCounts;
        this.includeExtendedDataTableNames = includeExtendedDataTableNames;
    }

    public boolean isIncludeAllProperties() {
        return includeAllProperties;
    }

    public Set<ByteSequence> getPropertyNamesToInclude() {
        return propertyNamesToInclude;
    }

    public boolean isIncludeAllPropertyMetadata() {
        return includeAllPropertyMetadata;
    }

    public Set<ByteSequence> getMetadataKeysToInclude() {
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

    public Set<String> getEdgeLabelsOfEdgeRefsToInclude() {
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

    @Override
    public String toString() {
        return "IteratorFetchHints{" +
            "includeAllProperties=" + includeAllProperties +
            ", propertyNamesToInclude=" + setOfByteSequencesToString(propertyNamesToInclude) +
            ", includeAllPropertyMetadata=" + includeAllPropertyMetadata +
            ", metadataKeysToInclude=" + setOfByteSequencesToString(metadataKeysToInclude) +
            ", includeHidden=" + includeHidden +
            ", includeAllEdgeRefs=" + includeAllEdgeRefs +
            ", includeOutEdgeRefs=" + includeOutEdgeRefs +
            ", includeInEdgeRefs=" + includeInEdgeRefs +
            ", ignoreAdditionalVisibilities=" + ignoreAdditionalVisibilities +
            ", edgeLabelsOfEdgeRefsToInclude=" + setToString(edgeLabelsOfEdgeRefsToInclude) +
            ", includeEdgeLabelsAndCounts=" + includeEdgeLabelsAndCounts +
            ", includeExtendedDataTableNames=" + includeExtendedDataTableNames +
            '}';
    }

    private String setOfByteSequencesToString(Set<ByteSequence> set) {
        if (set == null) {
            return "";
        }
        return set.stream()
            .map(ByteSequenceUtils::toString)
            .collect(Collectors.joining(","));
    }

    private String setToString(Set<String> set) {
        if (set == null) {
            return "";
        }
        return String.join(",", set);
    }
}
