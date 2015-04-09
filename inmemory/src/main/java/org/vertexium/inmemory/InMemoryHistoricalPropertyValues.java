package org.vertexium.inmemory;

import org.vertexium.HistoricalPropertyValue;
import org.vertexium.Metadata;
import org.vertexium.Property;
import org.vertexium.Visibility;
import org.vertexium.util.JavaSerializableUtils;

import java.util.*;

import static org.vertexium.util.IterableUtils.toList;

public class InMemoryHistoricalPropertyValues {
    private Map<String, Map<String, Map<String, SortedSet<HistoricalPropertyValue>>>> historicalPropertyValues = new HashMap<>();

    public void addProperty(Property property) {
        String propertyName = property.getName();
        String propertyKey = property.getKey();
        String visibilityString = property.getVisibility().getVisibilityString();
        long timestamp = property.getTimestamp();

        SortedSet<HistoricalPropertyValue> valuesByVisibility = getHistoricalPropertyValues(propertyName, propertyKey, visibilityString);
        Object valueCopy = JavaSerializableUtils.copy(property.getValue());
        Metadata metadataCopy = JavaSerializableUtils.copy(property.getMetadata());
        Iterable<Visibility> propertyHiddenVisibilities = property.getHiddenVisibilities();
        Set<Visibility> hiddenVisibilities;
        if (propertyHiddenVisibilities == null) {
            hiddenVisibilities = null;
        } else {
            hiddenVisibilities = JavaSerializableUtils.copy(new HashSet<>(toList(propertyHiddenVisibilities)));
        }
        valuesByVisibility.add(new HistoricalPropertyValue(timestamp, valueCopy, metadataCopy, hiddenVisibilities));
    }

    private SortedSet<HistoricalPropertyValue> getHistoricalPropertyValues(String propertyName, String propertyKey, String visibilityString) {
        Map<String, Map<String, SortedSet<HistoricalPropertyValue>>> propertiesByName = historicalPropertyValues.get(propertyName);
        if (propertiesByName == null) {
            propertiesByName = new HashMap<>();
            historicalPropertyValues.put(propertyName, propertiesByName);
        }
        Map<String, SortedSet<HistoricalPropertyValue>> propertiesByKey = propertiesByName.get(propertyKey);
        if (propertiesByKey == null) {
            propertiesByKey = new HashMap<>();
            propertiesByName.put(propertyKey, propertiesByKey);
        }
        SortedSet<HistoricalPropertyValue> propertiesByVisibility = propertiesByKey.get(visibilityString);
        if (propertiesByVisibility == null) {
            propertiesByVisibility = new TreeSet<>();
            propertiesByKey.put(visibilityString, propertiesByVisibility);
        }
        return propertiesByVisibility;
    }

    public Iterable<HistoricalPropertyValue> get(String propertyKey, String propertyName, Visibility propertyVisibility) {
        Map<String, Map<String, SortedSet<HistoricalPropertyValue>>> propertiesByName = historicalPropertyValues.get(propertyName);
        if (propertiesByName == null) {
            return new ArrayList<>();
        }
        Map<String, SortedSet<HistoricalPropertyValue>> propertiesByKey = propertiesByName.get(propertyKey);
        if (propertiesByKey == null) {
            return new ArrayList<>();
        }
        SortedSet<HistoricalPropertyValue> propertiesByVisibility = propertiesByKey.get(propertyVisibility.getVisibilityString());
        if (propertiesByVisibility == null) {
            return new ArrayList<>();
        }
        return propertiesByVisibility;
    }

    public void update(InMemoryHistoricalPropertyValues newValues) {
        for (Map.Entry<String, Map<String, Map<String, SortedSet<HistoricalPropertyValue>>>> propertiesByName : newValues.historicalPropertyValues.entrySet()) {
            String propertyName = propertiesByName.getKey();
            for (Map.Entry<String, Map<String, SortedSet<HistoricalPropertyValue>>> propertiesByKey : propertiesByName.getValue().entrySet()) {
                String propertyKey = propertiesByKey.getKey();
                for (Map.Entry<String, SortedSet<HistoricalPropertyValue>> propertiesByVisibility : propertiesByKey.getValue().entrySet()) {
                    String propertyVisibility = propertiesByVisibility.getKey();
                    SortedSet<HistoricalPropertyValue> values = getHistoricalPropertyValues(propertyName, propertyKey, propertyVisibility);
                    for (HistoricalPropertyValue value : propertiesByVisibility.getValue()) {
                        values.add(value);
                    }
                }
            }
        }
    }
}
