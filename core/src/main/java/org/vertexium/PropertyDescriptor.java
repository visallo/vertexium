package org.vertexium;

import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

/**
 * Encapsulates the parameters necessary to perform a Property search
 */
public class PropertyDescriptor {
    private final String key;
    private final String name;
    private final Visibility visibility;

    public PropertyDescriptor(String key, String name, Visibility visibility) {
        this.key = key;
        this.name = name;
        this.visibility = visibility;
    }

    public static PropertyDescriptor from(String key, String name, Visibility visibility) {
        return new PropertyDescriptor(key, name, visibility);
    }

    public static PropertyDescriptor fromProperty(Property p) {
        return new PropertyDescriptor(p.getKey(), p.getName(), p.getVisibility());
    }

    public static PropertyDescriptor fromPropertyDeleteMutation(PropertyDeleteMutation p) {
        return new PropertyDescriptor(p.getKey(), p.getName(), p.getVisibility());
    }

    public static PropertyDescriptor fromPropertySoftDeleteMutation(PropertySoftDeleteMutation p) {
        return new PropertyDescriptor(p.getKey(), p.getName(), p.getVisibility());
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return "PropertyDescriptor{" +
            "key='" + key + '\'' +
            ", name='" + name + '\'' +
            ", visibility=" + visibility +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PropertyDescriptor that = (PropertyDescriptor) o;

        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        return visibility != null ? visibility.equals(that.visibility) : that.visibility == null;

    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (visibility != null ? visibility.hashCode() : 0);
        return result;
    }
}
