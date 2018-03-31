package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class InMemoryExtendedDataRow extends ExtendedDataRowBase {
    private final ExtendedDataRowId id;
    private ReadWriteLock propertiesLock = new ReentrantReadWriteLock();
    private Set<InMemoryProperty> properties = new HashSet<>();

    public InMemoryExtendedDataRow(ExtendedDataRowId id, FetchHints fetchHints) {
        super(fetchHints);
        this.id = id;
    }

    public boolean canRead(VisibilityEvaluator visibilityEvaluator) {
        propertiesLock.readLock().lock();
        try {
            return properties.stream().anyMatch(e -> e.canRead(visibilityEvaluator));
        } finally {
            propertiesLock.readLock().unlock();
        }
    }

    @Override
    public ExtendedDataRowId getId() {
        return id;
    }

    public InMemoryExtendedDataRow toReadable(VisibilityEvaluator visibilityEvaluator) {
        propertiesLock.readLock().lock();
        try {
            InMemoryExtendedDataRow row = new InMemoryExtendedDataRow(getId(), getFetchHints());
            for (InMemoryProperty column : properties) {
                if (column.canRead(visibilityEvaluator)) {
                    row.properties.add(column);
                }
            }
            return row;
        } finally {
            propertiesLock.readLock().unlock();
        }
    }

    public void addColumn(
            String propertyName,
            String key,
            Object value,
            long timestamp,
            Visibility visibility
    ) {
        propertiesLock.writeLock().lock();
        try {
            InMemoryProperty prop = new InMemoryProperty(propertyName, key, value, FetchHints.ALL, timestamp, visibility);
            properties.remove(prop);
            properties.add(prop);
        } finally {
            propertiesLock.writeLock().unlock();
        }
    }

    public void removeColumn(String columnName, String key, Visibility visibility) {
        propertiesLock.writeLock().lock();
        try {
            properties.removeIf(p ->
                    p.getName().equals(columnName)
                            && p.getVisibility().equals(visibility)
                            && ((key == null && p.getKey() == null) || (key != null && key.equals(p.getKey())))
            );
        } finally {
            propertiesLock.writeLock().unlock();
        }
    }

    @Override
    public Iterable<Property> getProperties() {
        propertiesLock.readLock().lock();
        try {
            return this.properties.stream().map(p -> (Property) p).collect(Collectors.toList());
        } finally {
            propertiesLock.readLock().unlock();
        }
    }

    private static class InMemoryProperty extends Property {
        private final String name;
        private final String key;
        private final long timestamp;
        private final Object value;
        private final Visibility visibility;
        private final ColumnVisibility columnVisibility;
        private final FetchHints fetchHints;

        public InMemoryProperty(
                String name,
                String key,
                Object value,
                FetchHints fetchHints,
                long timestamp,
                Visibility visibility
        ) {
            this.name = name;
            this.key = key;
            this.value = value;
            this.fetchHints = fetchHints;
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
        public FetchHints getFetchHints() {
            return fetchHints;
        }

        @Override
        public Visibility getVisibility() {
            return visibility;
        }

        @Override
        public Metadata getMetadata() {
            return new Metadata(getFetchHints());
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
