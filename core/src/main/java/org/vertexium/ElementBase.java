package org.vertexium;

import org.vertexium.mutation.ElementMutation;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.util.FilterIterable;

import java.util.ArrayList;

public abstract class ElementBase implements Element {
    private transient Property idProperty;
    private transient Property edgeLabelProperty;
    private transient Property outVertexIdProperty;
    private transient Property inVertexIdProperty;
    private transient Property inOrOutVertexIdProperty;
    private transient Property tableNameProperty;
    private transient Property rowIdProperty;
    private transient Property elementTypeProperty;
    private transient Property elementIdProperty;

    @Override
    public Property getProperty(String key, String name, Visibility visibility) {
        Property reservedProperty = getReservedProperty(name);
        if (reservedProperty != null) {
            return reservedProperty;
        }
        for (Property p : internalGetProperties(key, name)) {
            if (visibility == null) {
                return p;
            }
            if (!visibility.equals(p.getVisibility())) {
                continue;
            }
            return p;
        }
        return null;
    }

    protected Property getReservedProperty(String name) {
        if (ID_PROPERTY_NAME.equals(name)) {
            return getIdProperty();
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getEdgeLabelProperty();
        } else if (Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getOutVertexIdProperty();
        } else if (Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getInVertexIdProperty();
        } else if (Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getInOrOutVertexIdProperty();
        } else if (ExtendedDataRow.TABLE_NAME.equals(name) && this instanceof ExtendedDataRow) {
            return getTableNameProperty();
        } else if (ExtendedDataRow.ROW_ID.equals(name) && this instanceof ExtendedDataRow) {
            return getRowIdProperty();
        } else if (ExtendedDataRow.ELEMENT_ID.equals(name) && this instanceof ExtendedDataRow) {
            return getElementIdProperty();
        } else if (ExtendedDataRow.ELEMENT_TYPE.equals(name) && this instanceof ExtendedDataRow) {
            return getElementTypeProperty();
        }
        return null;
    }

    @Override
    public Property getProperty(String name, Visibility visibility) {
        return getProperty(ElementMutation.DEFAULT_KEY, name, visibility);
    }

    @Override
    public Iterable<Property> getProperties(String name) {
        return getProperties(null, name);
    }

    protected Iterable<Property> internalGetProperties(String key, String name) {
        getFetchHints().assertPropertyIncluded(name);
        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property property) {
                if (key != null && !property.getKey().equals(key)) {
                    return false;
                }
                return property.getName().equals(name);
            }
        };
    }

    @Override
    public Iterable<Property> getProperties(String key, String name) {
        Property reservedProperty = getReservedProperty(name);
        if (reservedProperty != null) {
            ArrayList<Property> result = new ArrayList<>();
            result.add(reservedProperty);
            return result;
        }
        getFetchHints().assertPropertyIncluded(name);
        return internalGetProperties(key, name);
    }

    protected Property getIdProperty() {
        if (idProperty == null) {
            idProperty = new MutablePropertyImpl(
                ElementMutation.DEFAULT_KEY,
                ID_PROPERTY_NAME, getId(),
                null,
                getTimestamp(),
                null,
                null,
                getFetchHints()
            );
        }
        return idProperty;
    }

    protected Property getEdgeLabelProperty() {
        if (edgeLabelProperty == null && this instanceof Edge) {
            String edgeLabel = ((Edge) this).getLabel();
            edgeLabelProperty = new MutablePropertyImpl(
                ElementMutation.DEFAULT_KEY,
                Edge.LABEL_PROPERTY_NAME,
                edgeLabel,
                null,
                getTimestamp(),
                null,
                null,
                getFetchHints()
            );
        }
        return edgeLabelProperty;
    }

    protected Property getOutVertexIdProperty() {
        if (outVertexIdProperty == null && this instanceof Edge) {
            String outVertexId = ((Edge) this).getVertexId(Direction.OUT);
            outVertexIdProperty = new MutablePropertyImpl(
                ElementMutation.DEFAULT_KEY,
                Edge.OUT_VERTEX_ID_PROPERTY_NAME,
                outVertexId,
                null,
                getTimestamp(),
                null,
                null,
                getFetchHints()
            );
        }
        return outVertexIdProperty;
    }

    protected Property getInVertexIdProperty() {
        if (inVertexIdProperty == null && this instanceof Edge) {
            String inVertexId = ((Edge) this).getVertexId(Direction.IN);
            inVertexIdProperty = new MutablePropertyImpl(
                ElementMutation.DEFAULT_KEY,
                Edge.IN_VERTEX_ID_PROPERTY_NAME,
                inVertexId,
                null,
                getTimestamp(),
                null,
                null,
                getFetchHints()
            );
        }
        return inVertexIdProperty;
    }

    protected Property getInOrOutVertexIdProperty() {
        if (inOrOutVertexIdProperty == null && this instanceof Edge) {
            String inVertexId = ((Edge) this).getVertexId(Direction.IN);
            String outVertexId = ((Edge) this).getVertexId(Direction.OUT);
            inOrOutVertexIdProperty = new MutablePropertyImpl(
                ElementMutation.DEFAULT_KEY,
                Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME,
                new EdgeVertexIds(outVertexId, inVertexId),
                null,
                getTimestamp(),
                null,
                null,
                getFetchHints()
            );
        }
        return inOrOutVertexIdProperty;
    }

    protected Property getTableNameProperty() {
        if (tableNameProperty == null && this instanceof ExtendedDataRow) {
            String tableName = ((ExtendedDataRow) this).getId().getTableName();
            tableNameProperty = new MutablePropertyImpl(
                ElementMutation.DEFAULT_KEY,
                ExtendedDataRow.TABLE_NAME,
                tableName,
                null,
                getTimestamp(),
                null,
                null,
                getFetchHints()
            );
        }
        return tableNameProperty;
    }

    protected Property getRowIdProperty() {
        if (rowIdProperty == null && this instanceof ExtendedDataRow) {
            String rowId = ((ExtendedDataRow) this).getId().getRowId();
            rowIdProperty = new MutablePropertyImpl(
                ElementMutation.DEFAULT_KEY,
                ExtendedDataRow.ROW_ID,
                rowId,
                null,
                getTimestamp(),
                null,
                null,
                getFetchHints()
            );
        }
        return rowIdProperty;
    }

    protected Property getElementTypeProperty() {
        if (elementTypeProperty == null && this instanceof ExtendedDataRow) {
            String elementType = ((ExtendedDataRow) this).getId().getElementType().name();
            elementTypeProperty = new MutablePropertyImpl(
                ElementMutation.DEFAULT_KEY,
                ExtendedDataRow.ELEMENT_TYPE,
                elementType,
                null,
                getTimestamp(),
                null,
                null,
                getFetchHints()
            );
        }
        return elementTypeProperty;
    }

    protected Property getElementIdProperty() {
        if (elementIdProperty == null && this instanceof ExtendedDataRow) {
            String elementId = ((ExtendedDataRow) this).getId().getElementId();
            elementIdProperty = new MutablePropertyImpl(
                ElementMutation.DEFAULT_KEY,
                ExtendedDataRow.ELEMENT_ID,
                elementId,
                null,
                getTimestamp(),
                null,
                null,
                getFetchHints()
            );
        }
        return elementIdProperty;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Element) {
            Element objElem = (Element) obj;
            return getId().equals(objElem.getId());
        }
        return super.equals(obj);
    }

    @Override
    public String toString() {
        if (this instanceof Edge) {
            Edge edge = (Edge) this;
            return getId() + ":[" + edge.getVertexId(Direction.OUT) + "-" + edge.getLabel() + "->" + edge.getVertexId(Direction.IN) + "]";
        }
        return getId();
    }
}
