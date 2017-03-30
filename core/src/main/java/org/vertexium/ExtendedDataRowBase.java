package org.vertexium;

import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.FilterIterable;

import java.util.Iterator;

public abstract class ExtendedDataRowBase implements ExtendedDataRow {
    @Override
    public abstract ExtendedDataRowId getId();

    @Override
    public Iterable<String> getPropertyNames() {
        return new ConvertingIterable<Property, String>(getProperties()) {
            @Override
            protected String convert(Property prop) {
                return prop.getName();
            }
        };
    }

    @Override
    public abstract Iterable<Property> getProperties();

    @Override
    public Property getProperty(String name) {
        return getProperty(null, name, null);
    }

    @Override
    public Object getPropertyValue(String name) {
        return getPropertyValue(null, name);
    }

    @Override
    public Property getProperty(String key, String name) {
        return getProperty(key, name, null);
    }

    @Override
    public Property getProperty(String key, String name, Visibility visibility) {
        for (Property property : getProperties()) {
            if (isMatch(property, key, name, visibility)) {
                return property;
            }
        }
        return null;
    }

    private boolean isMatch(Property property, String key, String name, Visibility visibility) {
        if (name != null && !property.getName().equals(name)) {
            return false;
        }
        if (key != null && !property.getKey().equals(key)) {
            return false;
        }
        if (visibility != null && !property.getVisibility().equals(visibility)) {
            return false;
        }
        return true;
    }

    @Override
    public Property getProperty(String name, Visibility visibility) {
        return getProperty(null, name, visibility);
    }

    @Override
    public Iterable<Property> getProperties(String name) {
        return getProperties(null, name);
    }

    @Override
    public Iterable<Property> getProperties(String key, String name) {
        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property prop) {
                return isMatch(prop, key, name, null);
            }
        };
    }

    @Override
    public Iterable<Object> getPropertyValues(String name) {
        return getPropertyValues(null, name);
    }

    @Override
    public Iterable<Object> getPropertyValues(String key, String name) {
        return new ConvertingIterable<Property, Object>(getProperties(key, name)) {
            @Override
            protected Object convert(Property prop) {
                return prop.getValue();
            }
        };
    }

    @Override
    public Object getPropertyValue(String key, String name) {
        Property prop = getProperty(key, name);
        if (prop == null) {
            return null;
        }
        return prop.getValue();
    }

    @Override
    public Object getPropertyValue(String name, int index) {
        return getPropertyValue(null, name, index);
    }

    @Override
    public Object getPropertyValue(String key, String name, int index) {
        Iterator<Object> values = getPropertyValues(key, name).iterator();
        while (values.hasNext() && index > 0) {
            values.next();
            index--;
        }
        if (!values.hasNext()) {
            return null;
        }
        return values.next();
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof ExtendedDataRow) {
            return getId().compareTo(((ExtendedDataRow) o).getId());
        }
        throw new ClassCastException("o must be an " + ExtendedDataRow.class.getName());
    }
}
