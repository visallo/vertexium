package org.neolumin.vertexium.blueprints;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.neolumin.vertexium.Authorizations;

public class VertexiumBlueprintsEdge extends VertexiumBlueprintsElement implements Edge {
    protected VertexiumBlueprintsEdge(VertexiumBlueprintsGraph graph, org.neolumin.vertexium.Edge edge, Authorizations authorizations) {
        super(graph, edge, authorizations);
    }

    public static Edge create(VertexiumBlueprintsGraph graph, org.neolumin.vertexium.Edge edge, Authorizations authorizations) {
        if (edge == null) {
            return null;
        }
        return new VertexiumBlueprintsEdge(graph, edge, authorizations);
    }

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        org.neolumin.vertexium.Direction sgDirection = VertexiumBlueprintsConvert.toVertexium(direction);
        Authorizations authorizations = getGraph().getAuthorizationsProvider().getAuthorizations();
        return VertexiumBlueprintsVertex.create(getGraph(), getVertexiumElement().getVertex(sgDirection, authorizations), authorizations);
    }

    @Override
    public String getLabel() {
        return getVertexiumElement().getLabel();
    }

    @Override
    public void remove() {
        getGraph().removeEdge(this);
    }

    @Override
    public org.neolumin.vertexium.Edge getVertexiumElement() {
        return (org.neolumin.vertexium.Edge) super.getVertexiumElement();
    }

    @Override
    public void setProperty(String propertyName, Object value) {
        if ("label".equals(propertyName)) {
            throw new IllegalArgumentException("Property Name cannot be \"label\"");
        }
        super.setProperty(propertyName, value);
    }
}
