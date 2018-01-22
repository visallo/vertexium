package org.vertexium;

import com.google.common.collect.Lists;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.FilterIterable;

import java.util.Iterator;

public abstract class ExtendedDataRowBase implements ExtendedDataRow {
    private transient Property rowIdProperty;
    private transient Property tableNameProperty;

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
    public Property getProperty(String key, String name, Visibility visibility) {
        if (ExtendedDataRow.ROW_ID.equals(name)) {
            return getRowIdProperty();
        } else if (ExtendedDataRow.TABLE_NAME.equals(name)) {
            return getTableNameProperty();
        }
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
        if (ExtendedDataRow.ROW_ID.equals(name)) {
            return Lists.newArrayList(getRowIdProperty());
        } else if (ExtendedDataRow.TABLE_NAME.equals(name)) {
            return Lists.newArrayList(getTableNameProperty());
        }

        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property prop) {
                return isMatch(prop, key, name, null);
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
        if (ExtendedDataRow.ROW_ID.equals(name)) {
            return getRowIdProperty().getValue();
        } else if (ExtendedDataRow.TABLE_NAME.equals(name)) {
            return getTableNameProperty().getValue();
        }

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

    protected Property getRowIdProperty() {
        if (rowIdProperty == null) {
            rowIdProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    ExtendedDataRow.ROW_ID,
                    getId().getRowId(),
                    null,
                    null,
                    null,
                    null,
                    FetchHint.ALL
            );
        }
        return rowIdProperty;
    }

    protected Property getTableNameProperty() {
        if (tableNameProperty == null) {
            tableNameProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    ExtendedDataRow.ROW_ID,
                    getId().getTableName(),
                    null,
                    null,
                    null,
                    null,
                    FetchHint.ALL
            );
        }
        return tableNameProperty;
    }
}
