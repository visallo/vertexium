package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;
import org.vertexium.util.ConvertingIterable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class InMemoryExtendedDataRow extends ExtendedDataRowBase {
    private final ExtendedDataRowId id;
    private Map<String, InMemoryProperty> properties = new HashMap<>();

    public InMemoryExtendedDataRow(ExtendedDataRowId id) {
        this.id = id;
    }

    public boolean canRead(VisibilityEvaluator visibilityEvaluator) {
        return properties.entrySet().stream().anyMatch(e -> e.getValue().canRead(visibilityEvaluator));
    }

    @Override
    public ExtendedDataRowId getId() {
        return id;
    }

    @Override
    public Object getPropertyValue(String propertyName) {
        InMemoryProperty property = properties.get(propertyName);
        if (property == null) {
            return null;
        }
        return property.getValue();
    }

    @Override
    public Set<String> getPropertyNames() {
        return properties.keySet();
    }

    public InMemoryExtendedDataRow toReadable(VisibilityEvaluator visibilityEvaluator) {
        InMemoryExtendedDataRow row = new InMemoryExtendedDataRow(getId());
        for (Map.Entry<String, InMemoryProperty> column : properties.entrySet()) {
            if (column.getValue().canRead(visibilityEvaluator)) {
                row.properties.put(column.getKey(), column.getValue());
            }
        }
        return row;
    }

    public void addColumn(String propertyName, Object value, long timestamp, Visibility visibility) {
        properties.put(propertyName, new InMemoryProperty(id.getTableName(), propertyName, value, timestamp, visibility));
    }

    @Override
    public Iterable<Property> getProperties() {
        return new ConvertingIterable<InMemoryProperty, Property>(this.properties.values()) {
            @Override
            protected Property convert(InMemoryProperty prop) {
                return prop;
            }
        };
    }

    @Override
    public Property getProperty(String name) {
        return this.properties.get(name);
    }

    private static class InMemoryProperty extends Property {
        private final String key;
        private final String name;
        private final long timestamp;
        private final Object value;
        private final Visibility visibility;
        private final ColumnVisibility columnVisibility;

        public InMemoryProperty(String key, String name, Object value, long timestamp, Visibility visibility) {
            this.key = key;
            this.name = name;
            this.value = value;
            this.timestamp = timestamp;
            this.visibility = visibility;
            this.columnVisibility = new ColumnVisibility(visibility.getVisibilityString());
        }

        public boolean canRead(VisibilityEvaluator visibilityEvaluator) {
            try {
                return visibilityEvaluator.evaluate(columnVisibility);
            } catch (VisibilityParseException e) {
                throw new VertexiumException("could not evaluate visibility " + visibility.getVisibilityString(), e);
            }
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getName() {
            return name;
        }

        public Object getValue() {
            return value;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public Visibility getVisibility() {
            return visibility;
        }

        @Override
        public Metadata getMetadata() {
            return new Metadata();
        }

        @Override
        public Iterable<Visibility> getHiddenVisibilities() {
            return new ArrayList<>();
        }

        @Override
        public boolean isHidden(Authorizations authorizations) {
            return false;
        }
    }
}
