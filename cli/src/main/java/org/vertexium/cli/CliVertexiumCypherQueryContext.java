package org.vertexium.cli;

import org.vertexium.*;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherNodePattern;
import org.vertexium.cypher.ast.model.CypherRelationshipPattern;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.mutation.ExistingElementMutation;

import java.util.Set;

public class CliVertexiumCypherQueryContext extends VertexiumCypherQueryContext {
    private static String labelPropertyName;

    public CliVertexiumCypherQueryContext(Graph graph, Authorizations authorizations) {
        super(graph, authorizations);
    }

    public static void setLabelPropertyName(String labelPropertyName) {
        CliVertexiumCypherQueryContext.labelPropertyName = labelPropertyName;
    }

    @Override
    public Visibility calculateVertexVisibility(CypherNodePattern nodePattern, ExpressionScope scope) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public String getLabelPropertyName() {
        return CliVertexiumCypherQueryContext.labelPropertyName;
    }

    @Override
    public <T extends Element> void setLabelProperty(ElementMutation<T> m, String label) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void removeLabel(ExistingElementMutation<Vertex> vertex, String label) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public <T extends Element> void setProperty(ElementMutation<T> m, String propertyName, Object value) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public void removeProperty(ElementMutation<Element> m, String propertyName) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public String calculateEdgeLabel(CypherRelationshipPattern relationshipPattern, Vertex outVertex, Vertex inVertex, ExpressionScope scope) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Visibility calculateEdgeVisibility(CypherRelationshipPattern relationshipPattern, Vertex outVertex, Vertex inVertex, ExpressionScope scope) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public boolean isLabelProperty(Property property) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public Set<String> getVertexLabels(Vertex vertex) {
        throw new VertexiumException("not implemented");
    }

    @Override
    public int getMaxUnboundedRange() {
        return 100;
    }
}
