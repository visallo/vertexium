package org.vertexium;

import com.google.common.collect.Lists;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.util.FilterIterable;

public abstract class ExtendedDataRowBase implements ExtendedDataRow {
    private final Graph graph;
    private final FetchHints fetchHints;
    private final User user;
    private transient Property rowIdProperty;
    private transient Property tableNameProperty;
    private transient Property elementIdProperty;
    private transient Property elementTypeProperty;

    protected ExtendedDataRowBase(Graph graph, FetchHints fetchHints, User user) {
        this.graph = graph;
        this.fetchHints = fetchHints;
        this.user = user;
    }

    @Override
    public Iterable<Property> getProperties(String name) {
        if (isInternalPropertyName(name)) {
            return Lists.newArrayList(getInternalProperty(name));
        }
        getFetchHints().assertPropertyIncluded(name);
        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property prop) {
                return isMatch(prop, name);
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

    private boolean isMatch(Property property, String name) {
        if (name != null && !property.getName().equals(name)) {
            return false;
        }
        return true;
    }

    private Property getInternalProperty(String name) {
        if (ExtendedDataRow.ROW_ID.equals(name)) {
            return getRowIdProperty();
        } else if (ExtendedDataRow.TABLE_NAME.equals(name)) {
            return getTableNameProperty();
        } else if (ExtendedDataRow.ELEMENT_ID.equals(name)) {
            return getElementIdProperty();
        } else if (ExtendedDataRow.ELEMENT_TYPE.equals(name)) {
            return getElementTypeProperty();
        }
        throw new VertexiumException("Not an internal property name: " + name);
    }

    protected boolean isInternalPropertyName(String name) {
        if (ExtendedDataRow.ROW_ID.equals(name)
            || ExtendedDataRow.TABLE_NAME.equals(name)
            || ExtendedDataRow.ELEMENT_ID.equals(name)
            || ExtendedDataRow.ELEMENT_TYPE.equals(name)) {
            return true;
        }
        return false;
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
                FetchHints.ALL
            );
        }
        return rowIdProperty;
    }

    protected Property getTableNameProperty() {
        if (tableNameProperty == null) {
            tableNameProperty = new MutablePropertyImpl(
                ElementMutation.DEFAULT_KEY,
                ExtendedDataRow.TABLE_NAME,
                getId().getTableName(),
                null,
                null,
                null,
                null,
                FetchHints.ALL
            );
        }
        return tableNameProperty;
    }

    protected Property getElementIdProperty() {
        if (elementIdProperty == null) {
            elementIdProperty = new MutablePropertyImpl(
                ElementMutation.DEFAULT_KEY,
                ExtendedDataRow.ELEMENT_ID,
                getId().getElementId(),
                null,
                null,
                null,
                null,
                FetchHints.ALL
            );
        }
        return elementIdProperty;
    }

    protected Property getElementTypeProperty() {
        if (elementTypeProperty == null) {
            elementTypeProperty = new MutablePropertyImpl(
                ElementMutation.DEFAULT_KEY,
                ExtendedDataRow.ELEMENT_TYPE,
                getId().getElementType().name(),
                null,
                null,
                null,
                null,
                FetchHints.ALL
            );
        }
        return elementTypeProperty;
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    @Override
    public Graph getGraph() {
        return graph;
    }

    @Override
    public User getUser() {
        return user;
    }
}
