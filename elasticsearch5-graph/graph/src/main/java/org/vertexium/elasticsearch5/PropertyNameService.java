package org.vertexium.elasticsearch5;

import org.vertexium.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PropertyNameService {
    public static final String FIELDNAME_DOT_REPLACEMENT = "-_-";
    public static final Pattern PROPERTY_NAME_PATTERN = Pattern.compile("^(.*?)(_([0-9a-f]{32}))?(_[a-z]|\\.[a-z]+)?$");
    public static final Pattern AGGREGATION_NAME_PATTERN = Pattern.compile("(.*?)_([0-9a-f]+)");
    private final PropertyNameVisibilitiesStore propertyNameVisibilitiesStore;

    public PropertyNameService(PropertyNameVisibilitiesStore propertyNameVisibilitiesStore) {
        this.propertyNameVisibilitiesStore = propertyNameVisibilitiesStore;
    }

    public String addVisibilityToPropertyName(String propertyName, Visibility propertyVisibility) {
        String visibilityHash = getVisibilityHash(propertyName, propertyVisibility);
        return propertyName + "_" + visibilityHash;
    }

    private String getVisibilityHash(String propertyName, Visibility visibility) {
        return propertyNameVisibilitiesStore.getHash(propertyName, visibility);
    }

    public String replaceFieldnameDots(String fieldName) {
        return fieldName.replace(".", FIELDNAME_DOT_REPLACEMENT);
    }

    protected void addPropertyNameVisibility(IndexInfo indexInfo, String propertyName, Visibility propertyVisibility) {
        String propertyNameNoVisibility = removeVisibilityFromPropertyName(propertyName);
        if (propertyVisibility != null) {
            propertyNameVisibilitiesStore.getHash(propertyNameNoVisibility, propertyVisibility);
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

    public String getPropertyVisibilityHashFromPropertyName(String propertyName) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(propertyName);
        if (m.matches()) {
            return m.group(3);
        }
        throw new VertexiumException("Could not match property name: " + propertyName);
    }

    public Collection<String> getQueryableExtendedDataVisibilities(User user) {
        return propertyNameVisibilitiesStore.getHashes(user);
    }

    public Collection<String> getQueryableElementTypeVisibilityPropertyNames(User user) {
        Set<String> propertyNames = new HashSet<>();
        for (String hash : propertyNameVisibilitiesStore.getHashes(FieldNames.ELEMENT_TYPE, user)) {
            propertyNames.add(FieldNames.ELEMENT_TYPE + "_" + hash);
        }
        if (propertyNames.size() == 0) {
            throw new VertexiumNoMatchingPropertiesException("No queryable " + FieldNames.ELEMENT_TYPE + " for authorizations " + user);
        }
        return propertyNames;
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

    public String[] getPropertyNames(String propertyName, User user) {
        String[] allMatchingPropertyNames = getAllMatchingPropertyNames(propertyName, user);
        return Arrays.stream(allMatchingPropertyNames)
            .map(this::replaceFieldnameDots)
            .collect(Collectors.toList())
            .toArray(new String[allMatchingPropertyNames.length]);
    }

    public String[] getAllMatchingPropertyNames(String propertyName, User user) {
        if (Element.ID_PROPERTY_NAME.equals(propertyName)
            || Edge.LABEL_PROPERTY_NAME.equals(propertyName)
            || Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)
            || Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(propertyName)
            || Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)
            || ExtendedDataRow.ELEMENT_TYPE.equals(propertyName)
            || ExtendedDataRow.ELEMENT_ID.equals(propertyName)
            || ExtendedDataRow.TABLE_NAME.equals(propertyName)
            || ExtendedDataRow.ROW_ID.equals(propertyName)) {
            return new String[]{propertyName};
        }
        Collection<String> hashes = this.propertyNameVisibilitiesStore.getHashes(propertyName, user);
        return addHashesToPropertyName(propertyName, hashes);
    }

    public String getAggregationName(String name) {
        Matcher m = AGGREGATION_NAME_PATTERN.matcher(name);
        if (m.matches()) {
            return m.group(1);
        }
        throw new VertexiumException("Could not get aggregation name from: " + name);
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
}
