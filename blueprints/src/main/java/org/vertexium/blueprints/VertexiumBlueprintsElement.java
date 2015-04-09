package org.vertexium.blueprints;

import com.tinkerpop.blueprints.Element;
import org.vertexium.Authorizations;
import org.vertexium.Property;
import org.vertexium.Visibility;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public abstract class VertexiumBlueprintsElement implements Element {
    private static final String DEFAULT_PROPERTY_ID = "";
    private final org.vertexium.Element element;
    private final Authorizations authorizations;
    private final VertexiumBlueprintsGraph graph;

    protected VertexiumBlueprintsElement(VertexiumBlueprintsGraph graph, org.vertexium.Element element, Authorizations authorizations) {
        this.graph = graph;
        this.element = element;
        this.authorizations = authorizations;
    }

    @Override
    public <T> T getProperty(String key) {
        Iterator<Object> values = getVertexiumElement().getPropertyValues(key).iterator();
        if (values.hasNext()) {
            return (T) values.next();
        }
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        Set<String> propertyKeys = new HashSet<String>();
        for (Property property : getVertexiumElement().getProperties()) {
            propertyKeys.add(property.getName());
        }
        return propertyKeys;
    }

    @Override
    public void setProperty(String propertyName, Object value) {
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null.");
        }
        if (propertyName == null) {
            throw new IllegalArgumentException("Property Name cannot be null.");
        }
        if ("id".equals(propertyName)) {
            throw new IllegalArgumentException("Property Name cannot be \"id\"");
        }
        if ("".equals(propertyName)) {
            throw new IllegalArgumentException("Property Name cannot be empty.");
        }
        Visibility visibility = getGraph().getVisibilityProvider().getVisibilityForProperty(propertyName, value);
        getVertexiumElement().setProperty(propertyName, value, visibility, authorizations);
    }

    @Override
    public <T> T removeProperty(String key) {
        T old = getProperty(key);
        getVertexiumElement().deleteProperty(DEFAULT_PROPERTY_ID, key, authorizations);
        return old;
    }

    @Override
    public abstract void remove();

    @Override
    public Object getId() {
        return getVertexiumElement().getId();
    }

    public VertexiumBlueprintsGraph getGraph() {
        return graph;
    }

    public org.vertexium.Element getVertexiumElement() {
        return element;
    }

    @Override
    public int hashCode() {
        return getVertexiumElement().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VertexiumBlueprintsElement) {
            return getVertexiumElement().equals(((VertexiumBlueprintsElement) obj).getVertexiumElement());
        }
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return getVertexiumElement().toString();
    }
}
