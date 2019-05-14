package org.vertexium;

import com.google.common.collect.ImmutableSet;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.query.QueryableIterable;
import org.vertexium.util.FutureDeprecation;

import java.util.stream.Stream;

/**
 * An element on the graph. This can be either a vertex or edge.
 * <p/>
 * Elements also contain properties. These properties are unique given their key, name, and visibility.
 * For example a property with key "key1" and name "age" could have to values, one with visibility "a" and one
 * with visibility "b".
 */
public interface Element extends VertexiumObject, ElementLocation {
    /**
     * Meta property name used for operations such as sorting
     */
    String ID_PROPERTY_NAME = "__ID__";

    /**
     * id of the element.
     */
    String getId();

    /**
     * the visibility of the element.
     */
    Visibility getVisibility();

    /**
     * The timestamp of when this element was updated.
     */
    long getTimestamp();

    /**
     * Gets historical events about this element
     *
     * @param authorizations The authorizations required to load the events
     * @return An iterable of historic events
     */
    default Stream<HistoricalEvent> getHistoricalEvents(Authorizations authorizations) {
        return getHistoricalEvents(HistoricalEventsFetchHints.ALL, authorizations);
    }

    /**
     * Gets historical events about this element
     *
     * @param fetchHints     Fetch hints to filter historical events
     * @param authorizations The authorizations required to load the events
     * @return An iterable of historic events
     */
    default Stream<HistoricalEvent> getHistoricalEvents(HistoricalEventsFetchHints fetchHints, Authorizations authorizations) {
        return getHistoricalEvents(null, fetchHints, authorizations);
    }

    /**
     * Gets historical events about this element
     *
     * @param after          Find events after the given id
     * @param fetchHints     Fetch hints to filter historical events
     * @param authorizations The authorizations required to load the events
     * @return An iterable of historic events
     */
    @FutureDeprecation
    default Stream<HistoricalEvent> getHistoricalEvents(
        HistoricalEventId after,
        HistoricalEventsFetchHints fetchHints,
        Authorizations authorizations
    ) {
        return getHistoricalEvents(after, fetchHints, authorizations.getUser());
    }

    /**
     * Gets historical events about this element
     *
     * @param after      Find events after the given id
     * @param fetchHints Fetch hints to filter historical events
     * @param user       The user required to load the events
     * @return An iterable of historic events
     */
    Stream<HistoricalEvent> getHistoricalEvents(
        HistoricalEventId after,
        HistoricalEventsFetchHints fetchHints,
        User user
    );

    /**
     * Prepares a mutation to allow changing multiple property values at the same time. This method is similar to
     * Graph#prepareVertex(Visibility, Authorizations)
     * in that it allows multiple properties to be changed and saved in a single mutation.
     *
     * @return The mutation builder.
     */
    <T extends Element> ExistingElementMutation<T> prepareMutation();

    /**
     * Permanently deletes a property from the element. Only properties which you have access to can be deleted using
     * this method.
     *
     * @param property The property to delete.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#deleteProperty(Property)}
     */
    @Deprecated
    default void deleteProperty(Property property, Authorizations authorizations) {
        deleteProperty(property.getKey(), property.getName(), property.getVisibility(), authorizations);
    }

    /**
     * Permanently deletes a property given it's key and name from the element. Only properties which you have access
     * to can be deleted using this method.
     *
     * @param key  The property key.
     * @param name The property name.
     * @deprecated Use {@link org.vertexium.mutation.ExistingElementMutation#deleteProperties(String, String)}
     */
    @Deprecated
    default void deleteProperty(String key, String name, Authorizations authorizations) {
        deleteProperty(key, name, null, authorizations);
    }

    /**
     * Permanently deletes a property given it's key and name from the element. Only properties which you have access
     * to can be deleted using this method.
     *
     * @param key        The property key.
     * @param name       The property name.
     * @param visibility The property visibility.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#deleteProperty(String, String, Visibility)}
     */
    @Deprecated
    default void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        deleteProperty(key, name, visibility, authorizations.getUser());
    }

    /**
     * Permanently deletes a property given it's key and name from the element. Only properties which you have access
     * to can be deleted using this method.
     *
     * @param key        The property key.
     * @param name       The property name.
     * @param visibility The property visibility.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#deleteProperty(String, String, Visibility)}
     */
    @Deprecated
    default void deleteProperty(String key, String name, Visibility visibility, User user) {
        prepareMutation()
            .deleteProperty(key, name, visibility)
            .save(user);
    }

    /**
     * Permanently deletes all properties with the given name that you have access to. Only properties which you have
     * access to will be deleted.
     *
     * @param name The name of the property to delete.
     * @deprecated Use {@link org.vertexium.mutation.ExistingElementMutation#deleteProperties(String)}
     */
    @Deprecated
    default void deleteProperties(String name, Authorizations authorizations) {
        for (Property p : getProperties(name)) {
            deleteProperty(p.getKey(), p.getName(), p.getVisibility(), authorizations);
        }
    }

    /**
     * Soft deletes a property given it's key and name from the element. Only properties which you have access
     * to can be soft deleted using this method.
     *
     * @param key  The property key.
     * @param name The property name.
     * @deprecated Use {@link org.vertexium.mutation.ExistingElementMutation#softDeleteProperties(String, String)}
     */
    @Deprecated
    default void softDeleteProperty(String key, String name, Authorizations authorizations) {
        softDeleteProperty(key, name, null, authorizations);
    }

    /**
     * Soft deletes a property given it's key and name from the element. Only properties which you have access
     * to can be soft deleted using this method.
     *
     * @param key       The property key.
     * @param name      The property name.
     * @param eventData Data to store with the soft delete
     * @deprecated Use {@link org.vertexium.mutation.ExistingElementMutation#softDeleteProperties(String, String, Object)}
     */
    @Deprecated
    default void softDeleteProperty(String key, String name, Object eventData, Authorizations authorizations) {
        softDeleteProperty(key, name, null, eventData, authorizations);
    }

    /**
     * Soft deletes a property given it's key and name from the element for a given visibility. Only properties which you have access
     * to can be soft deleted using this method.
     *
     * @param key        The property key.
     * @param name       The property name.
     * @param visibility The visibility string of the property to soft delete.
     * @deprecated Use {@link org.vertexium.mutation.ExistingElementMutation#softDeleteProperty(String, String, Visibility)}
     */
    @Deprecated
    default void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        softDeleteProperty(key, name, visibility, null, authorizations);
    }

    /**
     * Soft deletes a property given it's key and name from the element for a given visibility. Only properties which you have access
     * to can be soft deleted using this method.
     *
     * @param key        The property key.
     * @param name       The property name.
     * @param visibility The visibility string of the property to soft delete.
     * @param eventData  Data to store with the soft delete
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#softDeleteProperty(String, String, Visibility)}
     */
    @Deprecated
    default void softDeleteProperty(String key, String name, Visibility visibility, Object eventData, Authorizations authorizations) {
        softDeleteProperty(key, name, visibility, eventData, authorizations.getUser());
    }

    /**
     * Soft deletes a property given it's key and name from the element for a given visibility. Only properties which you have access
     * to can be soft deleted using this method.
     *
     * @param key        The property key.
     * @param name       The property name.
     * @param visibility The visibility string of the property to soft delete.
     * @param eventData  Data to store with the soft delete
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#softDeleteProperty(String, String, Visibility)}
     */
    @Deprecated
    default void softDeleteProperty(String key, String name, Visibility visibility, Object eventData, User user) {
        prepareMutation()
            .softDeleteProperty(key, name, visibility, eventData)
            .save(user);
    }

    /**
     * Soft deletes all properties with the given name that you have access to. Only properties which you have
     * access to will be soft deleted.
     *
     * @param name The name of the property to delete.
     * @deprecated Use {@link org.vertexium.mutation.ExistingElementMutation#softDeleteProperties(String)}
     */
    @Deprecated
    default void softDeleteProperties(String name, Authorizations authorizations) {
        softDeleteProperties(name, null, authorizations);
    }

    /**
     * Soft deletes all properties with the given name that you have access to. Only properties which you have
     * access to will be soft deleted.
     *
     * @param name      The name of the property to delete.
     * @param eventData Data to store with the soft delete
     * @deprecated Use {@link org.vertexium.mutation.ExistingElementMutation#softDeleteProperties(String, Object)}
     */
    @Deprecated
    default void softDeleteProperties(String name, Object eventData, Authorizations authorizations) {
        for (Property property : getProperties(name)) {
            softDeleteProperty(property.getKey(), property.getName(), property.getVisibility(), eventData, authorizations);
        }
    }

    /**
     * Gets the graph that this element belongs to.
     */
    Graph getGraph();

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#addPropertyValue(String, String, Object, Visibility)}
     */
    @Deprecated
    default void addPropertyValue(String key, String name, Object value, Visibility visibility, Authorizations authorizations) {
        prepareMutation().addPropertyValue(key, name, value, visibility).save(authorizations);
    }

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#addPropertyValue(String, String, Object, Metadata, Visibility)}
     */
    @Deprecated
    default void addPropertyValue(String key, String name, Object value, Metadata metadata, Visibility visibility, Authorizations authorizations) {
        prepareMutation().addPropertyValue(key, name, value, metadata, visibility).save(authorizations);
    }

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     *
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#setProperty(String, Object, Visibility)}
     */
    @Deprecated
    default void setProperty(String name, Object value, Visibility visibility, Authorizations authorizations) {
        prepareMutation().setProperty(name, value, visibility).save(authorizations);
    }

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     *
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#setProperty(String, Object, Metadata, Visibility)}
     */
    @Deprecated
    default void setProperty(String name, Object value, Metadata metadata, Visibility visibility, Authorizations authorizations) {
        prepareMutation().setProperty(name, value, metadata, visibility).save(authorizations);
    }

    /**
     * Gets the user used to get this element.
     */
    User getUser();

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param visibility         The visibility string under which this property is hidden.
     *                           This visibility can be a superset of the property visibility to mark
     *                           it as hidden for only a subset of authorizations.
     * @param authorizations     The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyHidden(String, String, Visibility, Visibility)}
     */
    @Deprecated
    default void markPropertyHidden(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        markPropertyHidden(key, name, propertyVisibility, null, visibility, null, authorizations);
    }

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param visibility         The visibility string under which this property is hidden.
     *                           This visibility can be a superset of the property visibility to mark
     *                           it as hidden for only a subset of authorizations.
     * @param eventData          Data to store with the hidden
     * @param authorizations     The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyHidden(String, String, Visibility, Visibility, Object)}
     */
    @Deprecated
    default void markPropertyHidden(String key, String name, Visibility propertyVisibility, Visibility visibility, Object eventData, Authorizations authorizations) {
        markPropertyHidden(key, name, propertyVisibility, null, visibility, eventData, authorizations);
    }

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param timestamp          The timestamp.
     * @param visibility         The visibility string under which this property is hidden.
     *                           This visibility can be a superset of the property visibility to mark
     *                           it as hidden for only a subset of authorizations.
     * @param authorizations     The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyHidden(String, String, Visibility, Long, Visibility)}
     */
    @Deprecated
    default void markPropertyHidden(
        String key,
        String name,
        Visibility propertyVisibility,
        Long timestamp,
        Visibility visibility,
        Authorizations authorizations
    ) {
        markPropertyHidden(key, name, propertyVisibility, timestamp, visibility, null, authorizations);
    }

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param timestamp          The timestamp.
     * @param visibility         The visibility string under which this property is hidden.
     *                           This visibility can be a superset of the property visibility to mark
     *                           it as hidden for only a subset of authorizations.
     * @param eventData          Data to store with the hidden
     * @param authorizations     The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyHidden(String, String, Visibility, Long, Visibility, Object)}
     */
    @Deprecated
    default void markPropertyHidden(
        String key,
        String name,
        Visibility propertyVisibility,
        Long timestamp,
        Visibility visibility,
        Object eventData,
        Authorizations authorizations
    ) {
        Iterable<Property> properties = getProperties(key, name);
        for (Property property : properties) {
            if (property.getVisibility().equals(propertyVisibility)) {
                markPropertyHidden(property, timestamp, visibility, eventData, authorizations);
                return;
            }
        }
        throw new IllegalArgumentException("Could not find property " + key + " : " + name + " : " + propertyVisibility);
    }

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param property       The property.
     * @param visibility     The visibility string under which this property is hidden.
     *                       This visibility can be a superset of the property visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param authorizations The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyHidden(Property, Visibility)}
     */
    @Deprecated
    default void markPropertyHidden(Property property, Visibility visibility, Authorizations authorizations) {
        markPropertyHidden(property, null, visibility, null, authorizations);
    }

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param property       The property.
     * @param visibility     The visibility string under which this property is hidden.
     *                       This visibility can be a superset of the property visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param eventData      Data to store with the hidden
     * @param authorizations The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyHidden(Property, Visibility, Object)}
     */
    @Deprecated
    default void markPropertyHidden(Property property, Visibility visibility, Object eventData, Authorizations authorizations) {
        markPropertyHidden(property, null, visibility, eventData, authorizations);
    }

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param property       The property.
     * @param timestamp      The timestamp.
     * @param visibility     The visibility string under which this property is hidden.
     *                       This visibility can be a superset of the property visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param authorizations The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyHidden(Property, Long, Visibility)}
     */
    @Deprecated
    default void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        markPropertyHidden(property, timestamp, visibility, null, authorizations);
    }

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param property       The property.
     * @param timestamp      The timestamp.
     * @param visibility     The visibility string under which this property is hidden.
     *                       This visibility can be a superset of the property visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param authorizations The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyHidden(Property, Long, Visibility, Object)}
     */
    @Deprecated
    default void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Object data, Authorizations authorizations) {
        prepareMutation()
            .markPropertyHidden(property, timestamp, visibility, data)
            .save(authorizations);
    }

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param visibility         The visibility string under which this property is now visible.
     * @param authorizations     The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyVisible(String, String, Visibility, Visibility)}
     */
    @Deprecated
    default void markPropertyVisible(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        markPropertyVisible(key, name, propertyVisibility, null, visibility, null, authorizations);
    }

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param visibility         The visibility string under which this property is now visible.
     * @param eventData          Data to store with the visible
     * @param authorizations     The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyVisible(String, String, Visibility, Visibility, Object)}
     */
    @Deprecated
    default void markPropertyVisible(String key, String name, Visibility propertyVisibility, Visibility visibility, Object eventData, Authorizations authorizations) {
        markPropertyVisible(key, name, propertyVisibility, null, visibility, eventData, authorizations);
    }

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param timestamp          The timestamp.
     * @param visibility         The visibility string under which this property is now visible.
     * @param authorizations     The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyVisible(String, String, Visibility, Long, Visibility)}
     */
    @Deprecated
    default void markPropertyVisible(
        String key,
        String name,
        Visibility propertyVisibility,
        Long timestamp,
        Visibility visibility,
        Authorizations authorizations
    ) {
        markPropertyVisible(key, name, propertyVisibility, timestamp, visibility, null, authorizations);
    }

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param timestamp          The timestamp.
     * @param visibility         The visibility string under which this property is now visible.
     * @param eventData          Data to store with the visible
     * @param authorizations     The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyVisible(String, String, Visibility, Long, Visibility, Object)}
     */
    @Deprecated
    default void markPropertyVisible(
        String key,
        String name,
        Visibility propertyVisibility,
        Long timestamp,
        Visibility visibility,
        Object eventData,
        Authorizations authorizations
    ) {
        prepareMutation()
            .markPropertyVisible(key, name, propertyVisibility, timestamp, visibility, eventData)
            .save(authorizations);
    }

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param property       The property.
     * @param visibility     The visibility string under which this property is now visible.
     * @param authorizations The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyVisible(Property, Visibility)}
     */
    @Deprecated
    default void markPropertyVisible(Property property, Visibility visibility, Authorizations authorizations) {
        markPropertyVisible(property, null, visibility, null, authorizations);
    }

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param property       The property.
     * @param visibility     The visibility string under which this property is now visible.
     * @param eventData      Data to store with the visible
     * @param authorizations The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyVisible(Property, Visibility, Object)}
     */
    @Deprecated
    default void markPropertyVisible(Property property, Visibility visibility, Object eventData, Authorizations authorizations) {
        markPropertyVisible(property, null, visibility, eventData, authorizations);
    }

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param property       The property.
     * @param timestamp      The timestamp.
     * @param visibility     The visibility string under which this property is now visible.
     * @param authorizations The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyVisible(Property, Long, Visibility)}
     */
    @Deprecated
    default void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        markPropertyVisible(property, timestamp, visibility, null, authorizations);
    }

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param property       The property.
     * @param timestamp      The timestamp.
     * @param visibility     The visibility string under which this property is now visible.
     * @param eventData      Data to store with the visible
     * @param authorizations The authorizations used.
     * @deprecated Use {@link org.vertexium.mutation.ElementMutation#markPropertyVisible(Property, Long, Visibility, Object)}
     */
    @Deprecated
    default void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Object eventData, Authorizations authorizations) {
        prepareMutation()
            .markPropertyVisible(property, timestamp, visibility, eventData)
            .save(authorizations);
    }

    /**
     * Given the supplied authorizations is this element hidden?
     *
     * @param authorizations the authorizations to check against.
     * @return true, if it would be hidden from those authorizations.
     */
    default boolean isHidden(Authorizations authorizations) {
        for (Visibility visibility : getHiddenVisibilities()) {
            if (authorizations.canRead(visibility)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets the list of hidden visibilities
     */
    Iterable<Visibility> getHiddenVisibilities();

    /**
     * Gets the list of extended data table names.
     */
    ImmutableSet<String> getExtendedDataTableNames();

    /**
     * Gets all the rows from an extended data table.
     *
     * @param tableName The name of the table to get rows from.
     * @return Iterable of all the rows.
     */
    default QueryableIterable<ExtendedDataRow> getExtendedData(String tableName) {
        return getExtendedData(tableName, getFetchHints());
    }

    /**
     * Gets all the rows from an extended data table.
     *
     * @param tableName The name of the table to get rows from.
     * @return Iterable of all the rows.
     */
    QueryableIterable<ExtendedDataRow> getExtendedData(String tableName, FetchHints fetchHints);

    /**
     * Fetch hints used when fetching this element.
     */
    FetchHints getFetchHints();

    @Override
    default int compareTo(Object o) {
        if (getClass().isInstance(o)) {
            return getId().compareTo(((Element) o).getId());
        }
        throw new ClassCastException("o must be an " + getClass().getName());
    }
}
