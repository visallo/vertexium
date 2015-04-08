package org.vertexium.elasticsearch;

public class ParentChildIndexInfo extends IndexInfo {
    private boolean propertyTypeDefined;

    public ParentChildIndexInfo(String indexName) {
        super(indexName);
    }

    public boolean isPropertyTypeDefined() {
        return propertyTypeDefined;
    }

    public void setPropertyTypeDefined(boolean propertyTypeDefined) {
        this.propertyTypeDefined = propertyTypeDefined;
    }
}
