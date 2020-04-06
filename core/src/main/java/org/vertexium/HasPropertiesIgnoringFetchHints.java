package org.vertexium;

public interface HasPropertiesIgnoringFetchHints {
    default Iterable<Property> getPropertiesIgnoringFetchHints(String name) {
        return getPropertiesIgnoringFetchHints(null, name);
    }

    Iterable<Property> getPropertiesIgnoringFetchHints(String key, String name);

    Iterable<Property> getPropertiesIgnoringFetchHints();
}
