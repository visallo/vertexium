package org.vertexium.elasticsearch2;

import org.vertexium.Visibility;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IndexInfo {
    private final String indexName;
    private boolean elementTypeDefined;
    private Map<String, PropertyInfo> propertyInfos = new HashMap<>();

    public IndexInfo(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public int hashCode() {
        return getIndexName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IndexInfo)) {
            return false;
        }
        IndexInfo otherIndexInfo = (IndexInfo) obj;
        return getIndexName().equals(otherIndexInfo.getIndexName());
    }

    public boolean isElementTypeDefined() {
        return elementTypeDefined;
    }

    public void setElementTypeDefined(boolean elementTypeDefined) {
        this.elementTypeDefined = elementTypeDefined;
    }

    public void addPropertyNameVisibility(String propertyName, Visibility visibility) {
        PropertyInfo propertyInfo = propertyInfos.get(propertyName);
        if (propertyInfo == null) {
            propertyInfo = new PropertyInfo();
            propertyInfos.put(propertyName, propertyInfo);
        }
        propertyInfo.addVisibility(visibility);
    }

    public boolean isPropertyDefined(String propertyName, Visibility visibility) {
        PropertyInfo propertyInfo = propertyInfos.get(propertyName);
        if (propertyInfo == null) {
            return false;
        }
        return propertyInfo.hasVisibility(visibility);
    }

    public boolean isPropertyDefined(String propertyName) {
        PropertyInfo propertyInfo = propertyInfos.get(propertyName);
        return propertyInfo != null;
    }

    private static class PropertyInfo {
        private Set<Visibility> visibilities = new HashSet<>();

        public void addVisibility(Visibility visibility) {
            visibilities.add(visibility);
        }

        public boolean hasVisibility(Visibility visibility) {
            return visibilities.contains(visibility);
        }
    }
}
