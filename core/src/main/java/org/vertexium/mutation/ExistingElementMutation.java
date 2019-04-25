package org.vertexium.mutation;

import org.vertexium.Element;
import org.vertexium.Property;
import org.vertexium.Visibility;

import java.util.List;

public interface ExistingElementMutation<T extends Element> extends ElementMutation<T> {
    /**
     * Alters the visibility of a property.
     *
     * @param property   The property to mutate.
     * @param visibility The new visibility.
     */
    ExistingElementMutation<T> alterPropertyVisibility(Property property, Visibility visibility);

    /**
     * Alters the visibility of a property.
     *
     * @param key        The key of a multivalued property.
     * @param name       The name of the property to alter the visibility of.
     * @param visibility The new visibility.
     */
    ExistingElementMutation<T> alterPropertyVisibility(String key, String name, Visibility visibility);

    /**
     * Alters the visibility of a property (assumes the property key is the DEFAULT).
     *
     * @param name       The name of the property to alter the visibility of.
     * @param visibility The new visibility.
     */
    ExistingElementMutation<T> alterPropertyVisibility(String name, Visibility visibility);

    /**
     * Gets the properties whose visibilities are being altered in this mutation.
     */
    List<AlterPropertyVisibility> getAlterPropertyVisibilities();

    /**
     * Alters the visibility of the element (vertex or edge).
     *
     * @param visibility The new visibility.
     */
    ExistingElementMutation<T> alterElementVisibility(Visibility visibility);

    /**
     * Get the new element visibility or null if not being altered in this mutation.
     */
    Visibility getNewElementVisibility();

    /**
     * Get the old element visibility.
     */
    Visibility getOldElementVisibility();

    /**
     * Sets a property metadata value on a property.
     *
     * @param property     The property to mutate.
     * @param metadataName The name of the metadata.
     * @param newValue     The new value.
     * @param visibility   The visibility of the metadata item
     */
    ExistingElementMutation<T> setPropertyMetadata(Property property, String metadataName, Object newValue, Visibility visibility);

    /**
     * Sets a property metadata value on a property.
     *
     * @param propertyKey  The key of a multivalued property.
     * @param propertyName The name of the property.
     * @param metadataName The name of the metadata.
     * @param newValue     The new value.
     * @param visibility   The visibility of the metadata item
     */
    ExistingElementMutation<T> setPropertyMetadata(String propertyKey, String propertyName, String metadataName, Object newValue, Visibility visibility);

    /**
     * Sets a property metadata value on a property.
     *
     * @param propertyName The name of the property.
     * @param metadataName The name of the metadata.
     * @param newValue     The new value.
     * @param visibility   The visibility of the metadata item
     */
    ExistingElementMutation<T> setPropertyMetadata(String propertyName, String metadataName, Object newValue, Visibility visibility);

    /**
     * Gets all of the property metadata changes that are part of this mutation.
     */
    List<SetPropertyMetadata> getSetPropertyMetadatas();

    /**
     * Permanently deletes all default properties with that name irregardless of visibility.
     *
     * @param name the property name to delete.
     */
    ExistingElementMutation<T> deleteProperties(String name);

    /**
     * Permanently deletes all properties with this key and name irregardless of visibility.
     *
     * @param key  the key of the property to delete.
     * @param name the name of the property to delete.
     */
    ExistingElementMutation<T> deleteProperties(String key, String name);

    /**
     * Soft deletes all default properties with that name irregardless of visibility.
     *
     * @param name the property name to delete.
     */
    default ExistingElementMutation<T> softDeleteProperties(String name) {
        return softDeleteProperties(name, (Object) null);
    }

    /**
     * Soft deletes all default properties with that name irregardless of visibility.
     *
     * @param name the property name to delete.
     * @param data Data to store with the soft delete
     */
    ExistingElementMutation<T> softDeleteProperties(String name, Object data);

    /**
     * Soft deletes all properties with this key and name irregardless of visibility.
     *
     * @param key  the key of the property to delete.
     * @param name the name of the property to delete.
     */
    default ExistingElementMutation<T> softDeleteProperties(String key, String name) {
        return softDeleteProperties(key, name, null);
    }

    /**
     * Soft deletes all properties with this key and name irregardless of visibility.
     *
     * @param key  the key of the property to delete.
     * @param name the name of the property to delete.
     * @param data Data to store with the soft delete
     */
    ExistingElementMutation<T> softDeleteProperties(String key, String name, Object data);

    /**
     * Gets the element this mutation is affecting.
     *
     * @return The element.
     */
    T getElement();
}
