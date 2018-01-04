package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class InMemoryExtendedDataRow extends ExtendedDataRowBase {
    private final ExtendedDataRowId id;
    private Set<InMemoryProperty> properties = new HashSet<>();

    public InMemoryExtendedDataRow(ExtendedDataRowId id) {
        this.id = id;
    }

    public boolean canRead(VisibilityEvaluator visibilityEvaluator) {
        return properties.stream().anyMatch(e -> e.canRead(visibilityEvaluator));
    }

    @Override
    public ExtendedDataRowId getId() {
        return id;
    }

    public InMemoryExtendedDataRow toReadable(VisibilityEvaluator visibilityEvaluator) {
        InMemoryExtendedDataRow row = new InMemoryExtendedDataRow(getId());
        for (InMemoryProperty column : properties) {
            if (column.canRead(visibilityEvaluator)) {
                row.properties.add(column);
            }
        }
        return row;
    }

    public void addColumn(String propertyName, String key, Object value, long timestamp, Visibility visibility) {
        properties.add(new InMemoryProperty(propertyName, key, value, timestamp, visibility));
    }

    public void removeColumn(String columnName, String key, Visibility visibility) {
        properties.removeIf(p ->
                p.getName().equals(columnName)
                        && p.getVisibility().equals(visibility)
                        && ((key == null && p.getKey() == null) || (key != null && key.equals(p.getKey())))
        );
    }

    @Override
    public Iterable<Property> getProperties() {
        return this.properties.stream().map(p -> (Property) p).collect(Collectors.toList());
    }

    private static class InMemoryProperty extends Property {
        private final String name;
        private final String key;
        private final long timestamp;
        private final Object value;
        private final Visibility visibility;
        private final ColumnVisibility columnVisibility;

        public InMemoryProperty(String name, String key, Object value, long timestamp, Visibility visibility) {
            this.name = name;
            this.key = key;
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
