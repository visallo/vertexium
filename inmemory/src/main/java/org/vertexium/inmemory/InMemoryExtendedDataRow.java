package org.vertexium.inmemory;

import com.google.common.collect.ImmutableSet;
import org.vertexium.*;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;
import org.vertexium.util.StreamUtils;

import java.nio.charset.StandardCharsets;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class InMemoryExtendedDataRow extends ExtendedDataRowBase {
    private final ExtendedDataRowId id;
    private final ReadWriteLock propertiesLock = new ReentrantReadWriteLock();
    private final Set<InMemoryProperty> properties = new HashSet<>();
    private final Set<ColumnVisibility> additionalVisibilities = new HashSet<>();

    public InMemoryExtendedDataRow(ExtendedDataRowId id, FetchHints fetchHints) {
        super(fetchHints);
        this.id = id;
    }

    public boolean canRead(VisibilityEvaluator visibilityEvaluator, FetchHints fetchHints) {
        propertiesLock.readLock().lock();
        try {
            if (!fetchHints.isIgnoreAdditionalVisibilities() && !canReadAdditionalVisibility(visibilityEvaluator)) {
                return false;
            }
            return properties.stream().anyMatch(e -> e.canRead(visibilityEvaluator));
        } finally {
            propertiesLock.readLock().unlock();
        }
    }

    @Override
    public ExtendedDataRowId getId() {
        return id;
    }

    public InMemoryExtendedDataRow toReadable(VisibilityEvaluator visibilityEvaluator, FetchHints fetchHints) {
        propertiesLock.readLock().lock();
        try {
            InMemoryExtendedDataRow row = new InMemoryExtendedDataRow(getId(), getFetchHints());
            if (!fetchHints.isIgnoreAdditionalVisibilities() && !canReadAdditionalVisibility(visibilityEvaluator)) {
                return null;
            }
            for (InMemoryProperty column : properties) {
                if (column.canRead(visibilityEvaluator)) {
                    row.properties.add(column);
                }
            }
            row.additionalVisibilities.addAll(additionalVisibilities);
            return row;
        } finally {
            propertiesLock.readLock().unlock();
        }
    }

    private boolean canReadAdditionalVisibility(VisibilityEvaluator visibilityEvaluator) {
        if (additionalVisibilities.size() == 0) {
            return true;
        }
        try {
            for (ColumnVisibility additionalVisibility : additionalVisibilities) {
                if (!visibilityEvaluator.evaluate(additionalVisibility)) {
                    return false;
                }
            }
        } catch (VisibilityParseException ex) {
            throw new VertexiumException("Could not evaluate visibility", ex);
        }
        return true;
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

    public void addAdditionalVisibility(String additionalVisibility) {
        propertiesLock.writeLock().lock();
        try {
            additionalVisibilities.add(new ColumnVisibility(additionalVisibility));
        } finally {
            propertiesLock.writeLock().unlock();
        }
    }

    public void deleteAdditionalVisibility(String additionalVisibility) {
        propertiesLock.writeLock().lock();
        try {
            additionalVisibilities.remove(new ColumnVisibility(additionalVisibility));
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

    @Override
    public ImmutableSet<String> getAdditionalVisibilities() {
        return additionalVisibilities.stream()
            .map(av -> new String(av.getExpression(), StandardCharsets.UTF_8))
            .collect(StreamUtils.toImmutableSet());
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
            return Metadata.create(getFetchHints());
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
