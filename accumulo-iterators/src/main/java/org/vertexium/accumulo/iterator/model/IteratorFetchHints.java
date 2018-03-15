package org.vertexium.accumulo.iterator.model;

import java.io.Serializable;
import java.util.Set;
import java.util.stream.Collectors;

public class IteratorFetchHints implements Serializable {
    private static final long serialVersionUID = -6302969731435846529L;
    private final boolean includeAllProperties;
    private final Set<String> propertyNamesToInclude;
    private final boolean includeAllPropertyMetadata;
    private final Set<String> metadataKeysToInclude;
    private final boolean includeHidden;
    private final boolean includeAllEdgeRefs;
    private final boolean includeOutEdgeRefs;
    private final boolean includeInEdgeRefs;
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
        this.edgeLabelsOfEdgeRefsToInclude = null;
        this.includeEdgeLabelsAndCounts = false;
        this.includeExtendedDataTableNames = false;
    }

    public IteratorFetchHints(
            boolean includeAllProperties,
            Set<String> propertyNamesToInclude,
            boolean includeAllPropertyMetadata,
            Set<String> metadataKeysToInclude,
            boolean includeHidden,
            boolean includeAllEdgeRefs,
            boolean includeOutEdgeRefs,
            boolean includeInEdgeRefs,
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
        this.edgeLabelsOfEdgeRefsToInclude = edgeLabelsOfEdgeRefsToInclude;
        this.includeEdgeLabelsAndCounts = includeEdgeLabelsAndCounts;
        this.includeExtendedDataTableNames = includeExtendedDataTableNames;
    }

    public boolean isIncludeAllProperties() {
        return includeAllProperties;
    }

    public Set<String> getPropertyNamesToInclude() {
        return propertyNamesToInclude;
    }

    public boolean isIncludeAllPropertyMetadata() {
        return includeAllPropertyMetadata;
    }

    public Set<String> getMetadataKeysToInclude() {
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

    @Override
    public String toString() {
        return "IteratorFetchHints{" +
                "includeAllProperties=" + includeAllProperties +
                ", propertyNamesToInclude=" + setToString(propertyNamesToInclude) +
                ", includeAllPropertyMetadata=" + includeAllPropertyMetadata +
                ", metadataKeysToInclude=" + setToString(metadataKeysToInclude) +
                ", includeHidden=" + includeHidden +
                ", includeAllEdgeRefs=" + includeAllEdgeRefs +
                ", includeOutEdgeRefs=" + includeOutEdgeRefs +
                ", includeInEdgeRefs=" + includeInEdgeRefs +
                ", edgeLabelsOfEdgeRefsToInclude=" + setToString(edgeLabelsOfEdgeRefsToInclude) +
                ", includeEdgeLabelsAndCounts=" + includeEdgeLabelsAndCounts +
                ", includeExtendedDataTableNames=" + includeExtendedDataTableNames +
                '}';
    }

    private String setToString(Set<String> set) {
        if (set == null) {
            return "";
        }
        return set.stream().collect(Collectors.joining(","));
    }
}
