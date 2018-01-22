package org.vertexium;

import org.vertexium.mutation.ElementMutation;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.util.FilterIterable;

import java.util.ArrayList;

public abstract class ElementBase implements Element {
    private transient Property idProperty;
    private transient Property edgeLabelProperty;

    @Override
    public Property getProperty(String key, String name, Visibility visibility) {
        if (ID_PROPERTY_NAME.equals(name)) {
            return getIdProperty();
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getEdgeLabelProperty();
        }
        for (Property p : getProperties(name)) {
            if (!p.getKey().equals(key)) {
                continue;
            }
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
    public Property getProperty(String name, Visibility visibility) {
        return getProperty(ElementMutation.DEFAULT_KEY, name, visibility);
    }

    @Override
    public Iterable<Property> getProperties(String name) {
        if (ID_PROPERTY_NAME.equals(name)) {
            ArrayList<Property> result = new ArrayList<>();
            result.add(getIdProperty());
            return result;
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            ArrayList<Property> result = new ArrayList<>();
            result.add(getEdgeLabelProperty());
            return result;
        }
        return internalGetProperties(name);
    }

    protected Iterable<Property> internalGetProperties(String name) {
        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property property) {
                return property.getName().equals(name);
            }
        };
    }

    @Override
    public Iterable<Property> getProperties(final String key, final String name) {
        if (ID_PROPERTY_NAME.equals(name)) {
            ArrayList<Property> result = new ArrayList<>();
            result.add(getIdProperty());
            return result;
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            ArrayList<Property> result = new ArrayList<>();
            result.add(getEdgeLabelProperty());
            return result;
        }
        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property property) {
                return property.getName().equals(name) && property.getKey().equals(key);
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
}
