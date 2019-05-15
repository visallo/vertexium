package org.vertexium;

import com.google.common.collect.Lists;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.util.FilterIterable;

import java.util.Iterator;

public abstract class ElementBase implements Element {
    private transient Property idProperty;
    private transient Property edgeLabelProperty;
    private transient Property outVertexIdProperty;
    private transient Property inVertexIdProperty;
    private transient Property inOrOutVertexIdProperty;

    @Override
    public Property getProperty(String key, String name, Visibility visibility) {
        if (isInternalPropertyName(name)) {
            return getInternalProperty(name);
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

    @Override
    public Property getProperty(String name) {
        if (isInternalPropertyName(name)) {
            return getInternalProperty(name);
        }
        Iterator<Property> propertiesWithName = internalGetProperties(null, name).iterator();
        if (propertiesWithName.hasNext()) {
            return propertiesWithName.next();
        }
        return null;
    }

    @Override
    public Iterable<Property> getProperties(String key, String name) {
        if (isInternalPropertyName(name)) {
            return Lists.newArrayList(getInternalProperty(name));
        }
        return internalGetProperties(key, name);
    }

    @Override
    public Iterable<Property> getProperties(String name) {
        if (isInternalPropertyName(name)) {
            return Lists.newArrayList(getInternalProperty(name));
        }
        return internalGetProperties(null, name);
    }

    private Property getInternalProperty(String name) {
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
        }
        throw new VertexiumException("Not an internal property: " + name);
    }

    protected boolean isInternalPropertyName(String name) {
        if (ID_PROPERTY_NAME.equals(name)
            || (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge)
            || (Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge)
            || (Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge)
            || (Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge)) {
            return true;
        }
        return false;
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
}
