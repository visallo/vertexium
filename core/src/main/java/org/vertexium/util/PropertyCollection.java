package org.vertexium.util;

import org.vertexium.Property;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class PropertyCollection {
    private final ConcurrentSkipListSet<Property> propertiesList = new ConcurrentSkipListSet<>();
    private final Map<String, ConcurrentSkipListMap<String, ConcurrentSkipListSet<Property>>> propertiesByNameAndKey = new HashMap<>();

    public Iterable<Property> getProperties() {
        return propertiesList;
    }

    public synchronized Iterable<Property> getProperties(String key, String name) {
        if (key == null) {
            return getProperties(name);
        }

        Map<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(name);
        if (propertiesByKey == null) {
            return new ArrayList<>();
        }
        ConcurrentSkipListSet<Property> properties = propertiesByKey.get(key);
        if (properties == null) {
            return new ArrayList<>();
        }
        return properties;
    }

    public synchronized Iterable<Property> getProperties(String name) {
        Map<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(name);
        if (propertiesByKey == null) {
            return new ArrayList<>();
        }
        List<Property> results = new ArrayList<>();
        for (ConcurrentSkipListSet<Property> properties : propertiesByKey.values()) {
            results.addAll(properties);
        }
        return results;
    }

    public synchronized Property getProperty(String name, int index) {
        Map<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(name);
        if (propertiesByKey == null) {
            return null;
        }
        for (ConcurrentSkipListSet<Property> properties : propertiesByKey.values()) {
            for (Property property : properties) {
                if (index == 0) {
                    return property;
                }
                index--;
            }
        }
        return null;
    }

    public synchronized Property getProperty(String key, String name, int index) {
        if (key == null) {
            return getProperty(name, index);
        }
        Map<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(name);
        if (propertiesByKey == null) {
            return null;
        }
        ConcurrentSkipListSet<Property> properties = propertiesByKey.get(key);
        if (properties == null) {
            return null;
        }
        for (Property property : properties) {
            if (index == 0) {
                return property;
            }
            index--;
        }
        return null;
    }

    public synchronized void addProperty(Property property) {
        ConcurrentSkipListMap<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(property.getName());
        if (propertiesByKey == null) {
            propertiesByKey = new ConcurrentSkipListMap<>();
            this.propertiesByNameAndKey.put(property.getName(), propertiesByKey);
        }
        ConcurrentSkipListSet<Property> properties = propertiesByKey.get(property.getKey());
        if (properties == null) {
            properties = new ConcurrentSkipListSet<>();
            propertiesByKey.put(property.getKey(), properties);
        }
        properties.add(property);
        this.propertiesList.add(property);
    }

    public synchronized void removeProperty(Property property) {
        Map<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(property.getName());
        if (propertiesByKey == null) {
            return;
        }
        ConcurrentSkipListSet<Property> properties = propertiesByKey.get(property.getKey());
        if (properties == null) {
            return;
        }
        properties.remove(property);
        this.propertiesList.remove(property);
    }

    public synchronized Iterable<Property> removeProperties(String name) {
        List<Property> removedProperties = new ArrayList<>();
        Map<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(name);
        if (propertiesByKey != null) {
            for (ConcurrentSkipListSet<Property> properties : propertiesByKey.values()) {
                for (Property property : properties) {
                    removedProperties.add(property);
                }
            }
        }

        for (Property property : removedProperties) {
            removeProperty(property);
        }

        return removedProperties;
    }
}
