package org.vertexium.cypher;

import org.vertexium.*;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.executionPlan.CreateNodePatternExecutionStep;
import org.vertexium.cypher.executionPlan.CreateRelationshipPatternExecutionStep;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.query.QueryResultsIterable;

import java.time.ZonedDateTime;
import java.util.List;
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
    private final ZonedDateTime now;

    public TestVertexiumCypherQueryContext(Graph graph, Authorizations authorizations) {
        super(graph, authorizations);
        now = super.getNow();
        getGraph().defineProperty(getLabelPropertyName())
            .dataType(String.class)
            .define();
    }

    @Override
    public Visibility calculateVertexVisibility(CreateNodePatternExecutionStep nodePattern, CypherResultRow row) {
        return VISIBILITY;
    }

    @Override
    public String getLabelPropertyName() {
        return LABEL_PROPERTY_NAME;
    }

    @Override
    public <T extends Element> void setLabelProperty(ElementMutation<T> m, String label) {
        QueryResultsIterable<Element> elements = getGraph().query(getAuthorizations())
            .has(getLabelPropertyName(), label)
            .limit(0L)
            .elements();
        if (elements.getTotalHits() == 0) {
            plusLabelCount++;
        }
        m.addPropertyValue(label, getLabelPropertyName(), label, VISIBILITY);
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
        if (m instanceof ExistingElementMutation) {
            if (((ExistingElementMutation<T>) m).getElement().getProperty(propertyName) != null) {
                minusPropertyCount++;
            }
        }
        m.setProperty(propertyName, value, VISIBILITY);
        plusPropertyCount++;
    }

    @Override
    public void setProperty(Element element, String propertyName, Object value) {
        if (element.getProperty(propertyName) != null) {
            minusPropertyCount++;
        }
        ExistingElementMutation<Element> m = element.prepareMutation();
        m.setProperty(propertyName, value, VISIBILITY);
        plusPropertyCount++;
        m.save(getAuthorizations());
    }

    @Override
    public void removeProperty(Element element, Property prop) {
        ExistingElementMutation<Element> m = element.prepareMutation();
        m.deleteProperty(prop);
        minusPropertyCount++;
        m.save(getAuthorizations());
    }

    @Override
    public void removeProperty(Element element, String propName) {
        ExistingElementMutation<Element> m = element.prepareMutation();
        m.deleteProperties(propName);
        minusPropertyCount++;
        m.save(getAuthorizations());
    }

    @Override
    public String calculateEdgeLabel(CreateRelationshipPatternExecutionStep relationshipPattern, Vertex outVertex, Vertex inVertex, CypherResultRow row) {
        List<String> relTypeNames = relationshipPattern.getRelTypeNames();
        if (relTypeNames == null || relTypeNames.size() == 0) {
            return DEFAULT_EDGE_LABEL;
        }
        if (relTypeNames.size() == 1) {
            return normalizeLabelName(relTypeNames.get(0));
        }
        throw new VertexiumException("too many labels specified. expected 0 or 1 found " + relTypeNames.size());
    }

    @Override
    public Visibility calculateEdgeVisibility(CreateRelationshipPatternExecutionStep relationshipPattern, Vertex outVertex, Vertex inVertex, CypherResultRow row) {
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
    public void deleteEdge(Edge edge) {
        if (getGraph().doesEdgeExist(edge.getId(), getAuthorizations())) {
            Set<String> labels = stream(edge.getPropertyValues(getLabelPropertyName()))
                .map(Object::toString)
                .collect(Collectors.toSet());
            updateMinusPropertyCount(edge);

            super.deleteEdge(edge);
            minusRelationshipCount++;
            getGraph().flush();

            updateMinusLabelCount(labels);
        }
    }

    @Override
    public void deleteVertex(Vertex vertex) {
        if (getGraph().doesVertexExist(vertex.getId(), getAuthorizations())) {
            Set<String> labels = stream(vertex.getPropertyValues(getLabelPropertyName()))
                .map(Object::toString)
                .collect(Collectors.toSet());
            updateMinusPropertyCount(vertex);

            for (Edge edge : vertex.getEdges(Direction.BOTH, getAuthorizations())) {
                deleteEdge(edge);
            }
            super.deleteVertex(vertex);
            minusNodeCount++;
            getGraph().flush();

            updateMinusLabelCount(labels);
        }
    }

    private void updateMinusPropertyCount(Element element) {
        for (Property property : element.getProperties()) {
            if (property.getName().equalsIgnoreCase(getLabelPropertyName())) {
                continue;
            }
            minusPropertyCount++;
        }
    }

    private void updateMinusLabelCount(Set<String> labels) {
        for (String label : labels) {
            QueryResultsIterable<Element> elements = getGraph().query(getAuthorizations())
                .has(getLabelPropertyName(), label)
                .limit(0L)
                .elements();
            if (elements.getTotalHits() == 0) {
                minusLabelCount++;
            }
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
    public String calculateVertexId(CreateNodePatternExecutionStep nodePattern, CypherResultRow row) {
        return String.format("%08x", nextVertexId++);
    }

    @Override
    public String calculateEdgeId(CreateRelationshipPatternExecutionStep relationshipPattern, CypherResultRow row) {
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

    @Override
    public void defineProperty(String propertyName, Object value) {
        PropertyDefinition propertyDefinition = new PropertyDefinition(propertyName, value.getClass(), TextIndexHint.ALL);
        getGraph().savePropertyDefinition(propertyDefinition);
    }

    @Override
    public ZonedDateTime getNow() {
        return now;
    }
}
