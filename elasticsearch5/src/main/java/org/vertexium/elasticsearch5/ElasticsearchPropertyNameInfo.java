package org.vertexium.elasticsearch5;

import org.vertexium.Graph;
import org.vertexium.Visibility;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticsearchPropertyNameInfo {
    public static final Pattern PROPERTY_NAME_PATTERN = Pattern.compile("^(.*?)(_([0-9a-f]{32}))?(_[a-z]|\\.[a-z]+)?$");
    private final String propertyName;
    private final Visibility propertyVisibility;

    private ElasticsearchPropertyNameInfo(String propertyName, Visibility propertyVisibility) {
        this.propertyName = propertyName;
        this.propertyVisibility = propertyVisibility;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Visibility getPropertyVisibility() {
        return propertyVisibility;
    }

    public static ElasticsearchPropertyNameInfo parse(
        Graph graph,
        PropertyNameVisibilitiesStore propertyNameVisibilitiesStore,
        String rawPropertyName
    ) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(rawPropertyName);
        if (!m.matches()) {
            return null;
        }

        String propertyName = m.group(1);
        String visibilityHash = m.group(2);
        Visibility propertyVisibility;
        if (visibilityHash == null) {
            propertyVisibility = null;
        } else {
            visibilityHash = visibilityHash.substring(1); // stop leading _
            propertyVisibility = propertyNameVisibilitiesStore.getVisibilityFromHash(graph, visibilityHash);
        }
        return new ElasticsearchPropertyNameInfo(propertyName, propertyVisibility);
    }
}
