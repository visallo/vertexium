package org.vertexium.blueprints;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.vertexium.Authorizations;

public class VertexiumBlueprintsEdge extends VertexiumBlueprintsElement implements Edge {
    protected VertexiumBlueprintsEdge(VertexiumBlueprintsGraph graph, org.vertexium.Edge edge, Authorizations authorizations) {
        super(graph, edge, authorizations);
    }

    public static Edge create(VertexiumBlueprintsGraph graph, org.vertexium.Edge edge, Authorizations authorizations) {
        if (edge == null) {
            return null;
        }
        return new VertexiumBlueprintsEdge(graph, edge, authorizations);
    }

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        org.vertexium.Direction sgDirection = VertexiumBlueprintsConvert.toVertexium(direction);
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
    public org.vertexium.Edge getVertexiumElement() {
        return (org.vertexium.Edge) super.getVertexiumElement();
    }

    @Override
    public void setProperty(String propertyName, Object value) {
        if ("label".equals(propertyName)) {
            throw new IllegalArgumentException("Property Name cannot be \"label\"");
        }
        super.setProperty(propertyName, value);
    }
}
