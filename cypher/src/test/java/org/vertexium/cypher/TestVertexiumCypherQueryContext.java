package org.vertexium.cypher;

import org.vertexium.*;
import org.vertexium.cypher.ast.model.*;
import org.vertexium.cypher.executor.ExpressionScope;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.mutation.ExistingElementMutation;

import java.util.Set;
import java.util.stream.Collectors;

import static org.vertexium.util.StreamUtils.stream;

public class TestVertexiumCypherQueryContext extends VertexiumCypherQueryContext {
    public static final String LABEL_PROPERTY_NAME = "LABEL";
    public static final String DEFAULT_EDGE_LABEL = "EDGE_LABEL";
    public static final Visibility VISIBILITY = new Visibility("");
    private int plusNodeCount;
    private int plusRelationshipCount;
    private int plusLabelCount;
    private int plusPropertyCount;
    private int minusNodeCount;
    private int minusRelationshipCount;
    private int minusLabelCount;
    private int minusPropertyCount;
    private int nextVertexId;
    private int nextEdgeId;

    public TestVertexiumCypherQueryContext(Graph graph, Authorizations authorizations) {
        super(graph, authorizations);
    }

    @Override
    public Visibility calculateVertexVisibility(CypherNodePattern nodePattern, ExpressionScope scope) {
        return VISIBILITY;
    }

    @Override
    public String getLabelPropertyName() {
        return LABEL_PROPERTY_NAME;
    }

    @Override
    public <T extends Element> void setLabelProperty(ElementMutation<T> m, String label) {
        m.addPropertyValue(label, getLabelPropertyName(), label, VISIBILITY);
        plusLabelCount++;
    }

    @Override
    public void removeLabel(ExistingElementMutation<Vertex> m, String label) {
        if (m.getElement().getProperty(label, getLabelPropertyName()) != null) {
            m.deleteProperties(label, getLabelPropertyName());
            minusLabelCount++;
        }
    }

    @Override
    public <T extends Element> void setProperty(ElementMutation<T> m, String propertyName, Object value) {
        if (value instanceof CypherAstBase) {
            throw new VertexiumException("Cannot set a value of type " + CypherAstBase.class.getName() + " into property");
        }
        m.setProperty(propertyName, value, VISIBILITY);
        plusPropertyCount++;
    }

    @Override
    public void removeProperty(ElementMutation<Element> m, String propertyName) {
        if (m instanceof ExistingElementMutation) {
            if (((ExistingElementMutation) m).getElement().getProperty(propertyName) == null) {
                return;
            }
        }
        m.deleteProperty(propertyName, VISIBILITY);
        minusPropertyCount++;
    }

    @Override
    public String calculateEdgeLabel(CypherRelationshipPattern relationshipPattern, Vertex outVertex, Vertex inVertex, ExpressionScope scope) {
        CypherListLiteral<CypherRelTypeName> relTypeNames = relationshipPattern.getRelTypeNames();
        if (relTypeNames == null || relTypeNames.size() == 0) {
            return DEFAULT_EDGE_LABEL;
        }
        if (relTypeNames.size() == 1) {
            return relTypeNames.get(0).getValue();
        }
        throw new VertexiumException("too many labels specified. expected 0 or 1 found " + relTypeNames.size());
    }

    @Override
    public Visibility calculateEdgeVisibility(CypherRelationshipPattern relationshipPattern, Vertex outVertex, Vertex inVertex, ExpressionScope scope) {
        return VISIBILITY;
    }

    @Override
    public boolean isLabelProperty(Property property) {
        return property.getName().equals(LABEL_PROPERTY_NAME);
    }

    @Override
    public Set<String> getVertexLabels(Vertex vertex) {
        return stream(vertex.getPropertyValues(LABEL_PROPERTY_NAME))
                .map(Object::toString)
                .collect(Collectors.toSet());
    }

    @Override
    public Edge saveEdge(ElementMutation<Edge> m) {
        Edge edge = super.saveEdge(m);
        if (!(m instanceof ExistingElementMutation)) {
            plusRelationshipCount++;
        }
        return edge;
    }

    @Override
    public Vertex saveVertex(ElementMutation<Vertex> m) {
        Vertex vertex = super.saveVertex(m);
        if (!(m instanceof ExistingElementMutation)) {
            plusNodeCount++;
        }
        return vertex;
    }

    @Override
    public int getMaxUnboundedRange() {
        return 100;
    }

    @Override
    public void deleteEdge(Edge edge) {
        if (getGraph().doesEdgeExist(edge.getId(), getAuthorizations())) {
            super.deleteEdge(edge);
            minusRelationshipCount++;
            getGraph().flush();
        }
    }

    @Override
    public void deleteVertex(Vertex vertex) {
        if (getGraph().doesVertexExist(vertex.getId(), getAuthorizations())) {
            for (Edge edge : vertex.getEdges(Direction.BOTH, getAuthorizations())) {
                deleteEdge(edge);
            }
            super.deleteVertex(vertex);
            minusNodeCount++;
            getGraph().flush();
        }
    }

    public void clearCounts() {
        plusNodeCount = 0;
        plusRelationshipCount = 0;
        plusLabelCount = 0;
        plusPropertyCount = 0;
        minusNodeCount = 0;
        minusRelationshipCount = 0;
        minusLabelCount = 0;
        minusPropertyCount = 0;
    }

    public int getPlusNodeCount() {
        return plusNodeCount;
    }

    public int getPlusRelationshipCount() {
        return plusRelationshipCount;
    }

    public int getPlusLabelCount() {
        return plusLabelCount;
    }

    public int getPlusPropertyCount() {
        return plusPropertyCount;
    }

    public int getMinusNodeCount() {
        return minusNodeCount;
    }

    public int getMinusRelationshipCount() {
        return minusRelationshipCount;
    }

    public int getMinusLabelCount() {
        return minusLabelCount;
    }

    public int getMinusPropertyCount() {
        return minusPropertyCount;
    }

    @Override
    public String calculateVertexId(CypherNodePattern nodePattern, ExpressionScope scope) {
        return String.format("%08x", nextVertexId++);
    }

    @Override
    public String calculateEdgeId(CypherRelationshipPattern relationshipPattern, ExpressionScope scope) {
        return String.format("e%08x", nextEdgeId++);
    }

    @Override
    public String normalizeLabelName(String labelName) {
        if (labelName.equals("alternativeLabelName")) {
            return "labelName";
        }
        return super.normalizeLabelName(labelName);
    }

    @Override
    public String normalizePropertyName(String propertyName) {
        if (propertyName.equals("alternativePropertyName")) {
            return "propertyName";
        }
        return super.normalizePropertyName(propertyName);
    }
}
