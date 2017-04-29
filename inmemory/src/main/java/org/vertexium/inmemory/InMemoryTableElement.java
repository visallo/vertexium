package org.vertexium.inmemory;

import com.google.common.collect.Maps;
import org.vertexium.*;
import org.vertexium.HistoricalPropertyValue.HistoricalPropertyValueBuilder;
import org.vertexium.inmemory.mutations.*;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.FilterIterable;
import org.vertexium.util.IncreasingTime;
import org.vertexium.util.LookAheadIterable;

import java.io.Serializable;
import java.util.*;

public abstract class InMemoryTableElement<TElement extends InMemoryElement> implements Serializable {
    private final String id;
    private TreeSet<Mutation> mutations = new TreeSet<>();

    protected InMemoryTableElement(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void addAll(Mutation... newMutations) {
        Collections.addAll(mutations, newMutations);
    }

    public long getFirstTimestamp() {
        return findFirstMutation(ElementTimestampMutation.class).getTimestamp();
    }

    protected <T extends Mutation> T findLastMutation(Class<T> clazz) {
        T last = null;
        for (Mutation m : this.mutations) {
            if (clazz.isAssignableFrom(m.getClass())) {
                //noinspection unchecked
                last = (T) m;
            }
        }
        return last;
    }

    protected <T extends Mutation> T findFirstMutation(Class<T> clazz) {
        for (Mutation m : this.mutations) {
            if (clazz.isAssignableFrom(m.getClass())) {
                //noinspection unchecked
                return (T) m;
            }
        }
        return null;
    }

    protected <T extends Mutation> Iterable<T> findMutations(final Class<T> clazz) {
        return new ConvertingIterable<Mutation, T>(new FilterIterable<Mutation>(this.mutations) {
            @Override
            protected boolean isIncluded(Mutation m) {
                return clazz.isAssignableFrom(m.getClass());
            }
        }) {
            @Override
            protected T convert(Mutation o) {
                //noinspection unchecked
                return (T) o;
            }
        };
    }

    public Visibility getVisibility() {
        return findLastMutation(AlterVisibilityMutation.class).getNewVisibility();
    }

    public long getTimestamp() {
        return findLastMutation(ElementTimestampMutation.class).getTimestamp();
    }

    private List<PropertyMutation> findPropertyMutations(Property p) {
        return findPropertyMutations(p.getKey(), p.getName(), p.getVisibility());
    }

    public Property deleteProperty(String key, String name, Authorizations authorizations) {
        return deleteProperty(key, name, null, authorizations);
    }

    public Property getProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        List<PropertyMutation> propertyMutations = findPropertyMutations(key, name, visibility);
        if (propertyMutations == null || propertyMutations.size() == 0) {
            return null;
        }
        return toProperty(propertyMutations, true, authorizations);
    }

    public Property deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        Property p = getProperty(key, name, visibility, authorizations);
        if (p != null) {
            deleteProperty(p);
        }
        return p;
    }

    protected void deleteProperty(Property p) {
        List<PropertyMutation> propertyMutations = findPropertyMutations(p);
        this.mutations.removeAll(propertyMutations);
    }

    private List<PropertyMutation> findPropertyMutations(String key, String name, Visibility visibility) {
        List<PropertyMutation> results = new ArrayList<>();
        for (Mutation m : this.mutations) {
            if (!(m instanceof PropertyMutation)) {
                continue;
            }
            PropertyMutation pm = (PropertyMutation) m;
            if ((key == null || pm.getPropertyKey().equals(key))
                    && (name == null || pm.getPropertyName().equals(name))
                    && (visibility == null || pm.getPropertyVisibility().equals(visibility))) {
                results.add(pm);
            }
        }
        return results;
    }

    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(
            String key,
            String name,
            Visibility visibility,
            Long startTime,
            Long endTime,
            Authorizations authorizations
    ) {
        List<PropertyMutation> propertyMutations = findPropertyMutations(key, name, visibility);
        List<HistoricalPropertyValue> historicalPropertyValues = new ArrayList<>();

        /*
         * There is the expectation that historical property values are a snapshot of the property in
         * time. This method attempts to reconstruct the property current state from mutations.
         */
        Map<String, HistoricalPropertyValueBuilder> currentPropertyBuilders = Maps.newHashMap();
        Set<Visibility> hiddenVisibilities = new HashSet<>();

        for (PropertyMutation m : propertyMutations) {
            String propertyIdentifier = m.getPropertyKey() + m.getPropertyName();
            HistoricalPropertyValueBuilder builder = currentPropertyBuilders.get(propertyIdentifier);
            if (builder == null) {
                builder = new HistoricalPropertyValueBuilder(m.getPropertyKey(), m.getPropertyName(), m.getTimestamp());
                currentPropertyBuilders.put(propertyIdentifier, builder);
            }

            if (startTime != null && m.getTimestamp() < startTime) {
                continue;
            }
            if (endTime != null && m.getTimestamp() > endTime) {
                continue;
            }
            if (!canRead(m.getVisibility(), authorizations)) {
                continue;
            }
            // Ignore workspace interactions to avoid duplicated entries
            if (m.getVisibility() != null && m.getPropertyVisibility().getVisibilityString().matches("(.*)WORKSPACE(.*)")) {
                continue;
            }

            if (m instanceof SoftDeletePropertyMutation) {
                builder.isDeleted(true);
                builder.timestamp(m.getTimestamp());
                historicalPropertyValues.add(builder.build());
            } else if (m instanceof AddPropertyMetadataMutation) {
                builder.metadata(((AddPropertyMetadataMutation) m).getMetadata());
                builder.timestamp(m.getTimestamp());
            } else if (m instanceof MarkPropertyHiddenMutation) {
                // Ignore
            } else if (m instanceof MarkPropertyVisibleMutation) {
                // Ignore
            } else if (m instanceof AddPropertyValueMutation) {
                AddPropertyValueMutation apvm = (AddPropertyValueMutation) m;
                Object value = apvm.getValue();
                value = loadIfStreamingPropertyValue(value, m.getTimestamp());

                builder.propertyVisibility(m.getPropertyVisibility())
                        .timestamp(m.getTimestamp())
                        .value(value)
                        .metadata(apvm.getMetadata())
                        .hiddenVisibilities(hiddenVisibilities)
                        .isDeleted(false);

                // Property modifications use a soft delete immediately followed by an AddPropertyValueMutation.
                // If the condition occurs, remove the delete event from the set.
                if (historicalPropertyValues.size() > 0) {
                    HistoricalPropertyValue last = historicalPropertyValues.get(historicalPropertyValues.size() - 1);
                    if (propertyIdentifier.equals(last.getPropertyKey() + last.getPropertyName()) && last.isDeleted()) {
                        historicalPropertyValues.remove(historicalPropertyValues.size() - 1);
                    }
                }

                historicalPropertyValues.add(builder.build());
            } else {
                throw new VertexiumException("Unhandled PropertyMutation: " + m.getClass().getName());
            }
        }

        Collections.reverse(historicalPropertyValues);
        return historicalPropertyValues;
    }

    public Iterable<Property> getProperties(final boolean includeHidden, Long endTime, final Authorizations authorizations) {
        final TreeMap<String, List<PropertyMutation>> propertiesMutations = new TreeMap<>();
        for (PropertyMutation m : findMutations(PropertyMutation.class)) {
            if (endTime != null && m.getTimestamp() > endTime) {
                continue;
            }

            String mapKey = toMapKey(m);
            List<PropertyMutation> propertyMutations = propertiesMutations.get(mapKey);
            if (propertyMutations == null) {
                propertyMutations = new ArrayList<>();
                propertiesMutations.put(mapKey, propertyMutations);
            }
            propertyMutations.add(m);
        }
        return new LookAheadIterable<List<PropertyMutation>, Property>() {
            @Override
            protected boolean isIncluded(List<PropertyMutation> src, Property property) {
                return property != null;
            }

            @Override
            protected Property convert(List<PropertyMutation> propertyMutations) {
                return toProperty(propertyMutations, includeHidden, authorizations);
            }

            @Override
            protected Iterator<List<PropertyMutation>> createIterator() {
                return propertiesMutations.values().iterator();
            }
        };
    }

    private Property toProperty(List<PropertyMutation> propertyMutations, boolean includeHidden, Authorizations authorizations) {
        String propertyKey = null;
        String propertyName = null;
        Object value = null;
        Metadata metadata = null;
        long timestamp = 0;
        Set<Visibility> hiddenVisibilities = new HashSet<>();
        Visibility visibility = null;
        boolean softDeleted = false;
        boolean hidden = false;
        for (PropertyMutation m : propertyMutations) {
            if (!canRead(m.getVisibility(), authorizations)) {
                continue;
            }

            propertyKey = m.getPropertyKey();
            propertyName = m.getPropertyName();
            visibility = m.getPropertyVisibility();
            if (m.getTimestamp() > timestamp) {
                timestamp = m.getTimestamp();
            }
            if (m instanceof AddPropertyValueMutation) {
                AddPropertyValueMutation apvm = (AddPropertyValueMutation) m;
                value = apvm.getValue();
                metadata = apvm.getMetadata();
                softDeleted = false;
            } else if (m instanceof AddPropertyMetadataMutation) {
                AddPropertyMetadataMutation apmm = (AddPropertyMetadataMutation) m;
                metadata = apmm.getMetadata();
            } else if (m instanceof SoftDeletePropertyMutation) {
                softDeleted = true;
            } else if (m instanceof MarkPropertyHiddenMutation) {
                hidden = true;
                hiddenVisibilities.add(m.getVisibility());
            } else if (m instanceof MarkPropertyVisibleMutation) {
                hidden = false;
                hiddenVisibilities.remove(m.getVisibility());
            } else {
                throw new VertexiumException("Unhandled PropertyMutation: " + m.getClass().getName());
            }
        }
        if (softDeleted) {
            return null;
        }
        if (!includeHidden && hidden) {
            return null;
        }
        if (propertyKey == null) {
            return null;
        }
        value = loadIfStreamingPropertyValue(value, timestamp);
        return new MutablePropertyImpl(propertyKey, propertyName, value, metadata, timestamp, hiddenVisibilities, visibility);
    }

    private Object loadIfStreamingPropertyValue(Object value, long timestamp) {
        if (value instanceof StreamingPropertyValueRef) {
            value = loadStreamingPropertyValue((StreamingPropertyValueRef) value, timestamp);
        }
        return value;
    }

    protected StreamingPropertyValue loadStreamingPropertyValue(StreamingPropertyValueRef<?> streamingPropertyValueRef, long timestamp) {
        // There's no need to have a Graph object for the pure in-memory implementation. Subclasses should override.
        return streamingPropertyValueRef.toStreamingPropertyValue(null, timestamp);
    }

    private String toMapKey(PropertyMutation m) {
        return m.getPropertyName() + m.getPropertyKey() + m.getPropertyVisibility().getVisibilityString();
    }

    public void appendSoftDeleteMutation(Long timestamp) {
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        this.mutations.add(new SoftDeleteMutation(timestamp));
    }

    public void appendMarkHiddenMutation(Visibility visibility) {
        long timestamp = IncreasingTime.currentTimeMillis();
        this.mutations.add(new MarkHiddenMutation(timestamp, visibility));
    }

    public void appendMarkVisibleMutation(Visibility visibility) {
        long timestamp = IncreasingTime.currentTimeMillis();
        this.mutations.add(new MarkVisibleMutation(timestamp, visibility));
    }

    public Property appendMarkPropertyHiddenMutation(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        Property prop = getProperty(key, name, propertyVisibility, authorizations);
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        this.mutations.add(new MarkPropertyHiddenMutation(key, name, propertyVisibility, timestamp, visibility));
        return prop;
    }

    public Property appendMarkPropertyVisibleMutation(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        Property prop = getProperty(key, name, propertyVisibility, authorizations);
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        this.mutations.add(new MarkPropertyVisibleMutation(key, name, propertyVisibility, timestamp, visibility));
        return prop;
    }

    public void appendSoftDeletePropertyMutation(String key, String name, Visibility propertyVisibility, Long timestamp) {
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        this.mutations.add(new SoftDeletePropertyMutation(timestamp, key, name, propertyVisibility));
    }

    public void appendAlterVisibilityMutation(Visibility newVisibility) {
        long timestamp = IncreasingTime.currentTimeMillis();
        this.mutations.add(new AlterVisibilityMutation(timestamp, newVisibility));
    }

    public void appendAddPropertyValueMutation(String key, String name, Object value, Metadata metadata, Visibility visibility, Long timestamp) {
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        this.mutations.add(new AddPropertyValueMutation(timestamp, key, name, value, metadata, visibility));
    }

    public void appendAddPropertyMetadataMutation(String key, String name, Metadata metadata, Visibility visibility, Long timestamp) {
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        this.mutations.add(new AddPropertyMetadataMutation(timestamp, key, name, metadata, visibility));
    }

    public void appendAlterEdgeLabelMutation(long timestamp, String newEdgeLabel) {
        this.mutations.add(new AlterEdgeLabelMutation(timestamp, newEdgeLabel));
    }

    protected List<Mutation> getFilteredMutations(boolean includeHidden, Long endTime, Authorizations authorizations) {
        List<Mutation> mutations = new ArrayList<>();
        for (Mutation m : this.mutations) {
            if (!canRead(m.getVisibility(), authorizations)) {
                continue;
            }
            if (endTime != null && m.getTimestamp() > endTime) {
                continue;
            }
            if (includeHidden) {
                if (m instanceof MarkHiddenMutation || m instanceof MarkPropertyHiddenMutation) {
                    continue;
                }
            }
            mutations.add(m);
        }
        return mutations;
    }

    public boolean canRead(Authorizations authorizations) {
        // this is just a shortcut so that we don't need to construct evaluators and visibility objects to check for an empty string.
        //noinspection SimplifiableIfStatement
        if (getVisibility().getVisibilityString().length() == 0) {
            return true;
        }

        return authorizations.canRead(getVisibility());
    }

    private static boolean canRead(Visibility visibility, Authorizations authorizations) {
        // this is just a shortcut so that we don't need to construct evaluators and visibility objects to check for an empty string.
        //noinspection SimplifiableIfStatement
        if (visibility.getVisibilityString().length() == 0) {
            return true;
        }
        return authorizations.canRead(visibility);
    }

    public Set<Visibility> getHiddenVisibilities() {
        Set<Visibility> results = new HashSet<>();
        for (Mutation m : this.mutations) {
            if (m instanceof MarkHiddenMutation) {
                results.add(m.getVisibility());
            } else if (m instanceof MarkVisibleMutation) {
                results.remove(m.getVisibility());
            }
        }
        return results;
    }

    public boolean isHidden(Authorizations authorizations) {
        for (Visibility visibility : getHiddenVisibilities()) {
            if (authorizations.canRead(visibility)) {
                return true;
            }
        }
        return false;
    }

    public TElement createElement(InMemoryGraph graph, Authorizations authorizations) {
        return createElement(graph, true, null, authorizations);
    }

    public final TElement createElement(InMemoryGraph graph, boolean includeHidden, Long endTime, Authorizations authorizations) {
        if (endTime != null && getFirstTimestamp() > endTime) {
            return null;
        }
        if (isDeleted(endTime, authorizations)) {
            return null;
        }
        return createElementInternal(graph, includeHidden, endTime, authorizations);
    }

    public boolean isDeleted(Long endTime, Authorizations authorizations) {
        boolean deleted = false;
        for (Mutation m : this.mutations) {
            if (!canRead(m.getVisibility(), authorizations)) {
                continue;
            }
            if (endTime != null && m.getTimestamp() > endTime) {
                continue;
            }
            if (m instanceof SoftDeleteMutation) {
                deleted = true;
            } else if (m instanceof ElementTimestampMutation) {
                deleted = false;
            }
        }
        return deleted;
    }

    protected abstract TElement createElementInternal(InMemoryGraph graph, boolean includeHidden, Long endTime, Authorizations authorizations);
}
