package org.vertexium.elasticsearch5;

import org.vertexium.*;
import org.vertexium.mutation.ExtendedDataMutation;
import org.vertexium.type.GeoShape;

import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.StreamSupport;

import static org.vertexium.elasticsearch5.Elasticsearch5SearchIndex.*;
import static org.vertexium.elasticsearch5.ElasticsearchPropertyNameInfo.PROPERTY_NAME_PATTERN;

public class PropertyNameService {
    private final PropertyNameVisibilitiesStore propertyNameVisibilitiesStore;

    public PropertyNameService(PropertyNameVisibilitiesStore propertyNameVisibilitiesStore) {
        this.propertyNameVisibilitiesStore = propertyNameVisibilitiesStore;
    }

    public String addVisibilityToPropertyName(Graph graph, Property property) {
        String propertyName = property.getName();
        Visibility propertyVisibility = property.getVisibility();
        return addVisibilityToPropertyName(graph, propertyName, propertyVisibility);
    }

    String addVisibilityToExtendedDataColumnName(Graph graph, ExtendedDataMutation extendedDataMutation) {
        String columnName = extendedDataMutation.getColumnName();
        Visibility propertyVisibility = extendedDataMutation.getVisibility();
        return addVisibilityToPropertyName(graph, columnName, propertyVisibility);
    }

    public String addVisibilityToPropertyName(Graph graph, String propertyName, Visibility propertyVisibility) {
        String visibilityHash = getVisibilityHash(graph, propertyName, propertyVisibility);
        return propertyName + "_" + visibilityHash;
    }

    private String getVisibilityHash(Graph graph, String propertyName, Visibility visibility) {
        return this.propertyNameVisibilitiesStore.getHash(graph, propertyName, visibility);
    }

    protected void addPropertyNameVisibility(Graph graph, IndexInfo indexInfo, String propertyName, Visibility propertyVisibility) {
        String propertyNameNoVisibility = removeVisibilityFromPropertyName(propertyName);
        if (propertyVisibility != null) {
            this.propertyNameVisibilitiesStore.getHash(graph, propertyNameNoVisibility, propertyVisibility);
        }
        indexInfo.addPropertyNameVisibility(propertyNameNoVisibility, propertyVisibility);
        indexInfo.addPropertyNameVisibility(propertyName, propertyVisibility);
    }

    protected String removeVisibilityFromPropertyName(String string) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(string);
        if (m.matches()) {
            string = m.group(1);
        }
        return string;
    }

    public String removeVisibilityFromPropertyNameWithTypeSuffix(String string) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(string);
        if (m.matches()) {
            if (m.groupCount() >= 4 && m.group(4) != null) {
                string = m.group(1) + m.group(4);
            } else {
                string = m.group(1);
            }
        }
        return string;
    }

    public String replaceFieldnameDots(String fieldName) {
        return fieldName.replace(".", FIELDNAME_DOT_REPLACEMENT);
    }

    public String[] getAllMatchingPropertyNames(Graph graph, String propertyName, User user) {
        if (Element.ID_PROPERTY_NAME.equals(propertyName)
            || Edge.LABEL_PROPERTY_NAME.equals(propertyName)
            || Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)
            || Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(propertyName)
            || Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            return new String[]{propertyName};
        }
        Collection<String> hashes = this.propertyNameVisibilitiesStore.getHashes(graph, propertyName, user);
        return addHashesToPropertyName(propertyName, hashes);
    }

    public String[] addHashesToPropertyName(String propertyName, Collection<String> hashes) {
        if (hashes.size() == 0) {
            return new String[0];
        }
        String[] results = new String[hashes.size()];
        int i = 0;
        for (String hash : hashes) {
            results[i++] = propertyName + "_" + hash;
        }
        return results;
    }

    public Collection<String> getQueryableExtendedDataVisibilities(Graph graph, User user) {
        return propertyNameVisibilitiesStore.getHashes(graph, user);
    }

    public Collection<String> getQueryableElementTypeVisibilityPropertyNames(Graph graph, User user) {
        Set<String> propertyNames = new HashSet<>();
        for (String hash : propertyNameVisibilitiesStore.getHashes(graph, ELEMENT_TYPE_FIELD_NAME, user)) {
            propertyNames.add(ELEMENT_TYPE_FIELD_NAME + "_" + hash);
        }
        if (propertyNames.size() == 0) {
            throw new VertexiumNoMatchingPropertiesException("No queryable " + ELEMENT_TYPE_FIELD_NAME + " for authorizations " + user);
        }
        return propertyNames;
    }

    public Collection<String> getQueryablePropertyNames(Graph graph, User user) {
        Set<String> propertyNames = new HashSet<>();
        for (PropertyDefinition propertyDefinition : graph.getPropertyDefinitions()) {
            List<String> queryableTypeSuffixes = getQueryableTypeSuffixes(propertyDefinition);
            if (queryableTypeSuffixes.size() == 0) {
                continue;
            }
            String propertyNameNoVisibility = removeVisibilityFromPropertyName(propertyDefinition.getPropertyName()); // could have visibility
            if (isReservedFieldName(propertyNameNoVisibility)) {
                continue;
            }
            for (String hash : propertyNameVisibilitiesStore.getHashes(graph, propertyNameNoVisibility, user)) {
                for (String typeSuffix : queryableTypeSuffixes) {
                    propertyNames.add(propertyNameNoVisibility + "_" + hash + typeSuffix);
                }
            }
        }
        return propertyNames;
    }

    private static List<String> getQueryableTypeSuffixes(PropertyDefinition propertyDefinition) {
        List<String> typeSuffixes = new ArrayList<>();
        if (propertyDefinition.getDataType() == String.class) {
            if (propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                typeSuffixes.add(EXACT_MATCH_PROPERTY_NAME_SUFFIX);
            }
            if (propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                typeSuffixes.add("");
            }
        } else if (GeoShape.class.isAssignableFrom(propertyDefinition.getDataType())) {
            typeSuffixes.add("");
        }
        return typeSuffixes;
    }

    protected static boolean isReservedFieldName(String fieldName) {
        return fieldName.startsWith("__");
    }

    public String getPropertyVisibilityHashFromPropertyName(String propertyName) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(propertyName);
        if (m.matches()) {
            return m.group(3);
        }
        throw new VertexiumException("Could not match property name: " + propertyName);
    }
}
