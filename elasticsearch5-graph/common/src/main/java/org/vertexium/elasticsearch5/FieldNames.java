package org.vertexium.elasticsearch5;

public class FieldNames {
    public static final String PROPERTIES_DATA = "__propertiesData";
    public static final String ADDITIONAL_VISIBILITY_DATA = "__additionalVisibilityData";
    public static final String HIDDEN_VISIBILITY_DATA = "__hiddenVisibilityData";
    public static final String SOFT_DELETE_DATA = "__softDeleteData";

    public static final String ELEMENT_ID = "__elementId";
    public static final String ELEMENT_TYPE = "__elementType";
    public static final String ELEMENT_VISIBILITY = "__elementVisibility";
    public static final String OUT_VERTEX_ID = "__outVertexId";
    public static final String IN_VERTEX_ID = "__inVertexId";
    public static final String EDGE_LABEL = "__edgeLabel";
    public static final String EXACT_MATCH = "exact";
    public static final String EXTENDED_DATA_TABLE_NAME = "__extendedDataTableName";
    public static final String EXTENDED_DATA_TABLE_ROW_ID = "__extendedDataRowId";
    public static final String EXTENDED_DATA_TABLE_PROPERTY_VISIBILITIES = "__extendedDataPropertyVisibilities";
    public static final String VISIBILITY = "__visibility";
    public static final String HIDDEN_ELEMENT = "__hidden";
    public static final String HIDDEN_PROPERTY = "__hidden_property";
    public static final String GRAPH_METADATA_NAME = "__graphMetadataName";
    public static final String GRAPH_METADATA_VALUE = "__graphMetadataValue";
    public static final String ADDITIONAL_VISIBILITY = "__additionalVisibility";
    public static final String EXACT_MATCH_PROPERTY_NAME_SUFFIX = "." + EXACT_MATCH;
    public static final String GEO_PROPERTY_NAME_SUFFIX = "_g";
    public static final String GEO_POINT_PROPERTY_NAME_SUFFIX = "_gp"; // Used for geo hash aggregation of geo points

    public static final String MUTATION_ELEMENT_TYPE = "__mutElementType";
    public static final String MUTATION_ELEMENT_ID = "__mutElementId";
    public static final String MUTATION_TIMESTAMP = "__mutTimestamp";
    public static final String MUTATION_OUT_VERTEX_ID = "__mutOutVertexId";
    public static final String MUTATION_IN_VERTEX_ID = "__mutInVertexId";
    public static final String MUTATION_EDGE_LABEL = "__mutEdgeLabel";
    public static final String MUTATION_TYPE = "__mutType";
    public static final String MUTATION_DATA = "__mutData";
}
