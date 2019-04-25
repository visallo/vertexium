package org.vertexium.cypher.executionPlan;

import org.vertexium.ElementType;
import org.vertexium.Vertex;
import org.vertexium.Visibility;
import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.ast.model.CypherDirection;
import org.vertexium.mutation.ElementMutation;

import java.util.List;

public class CreateRelationshipPatternExecutionStep extends CreateElementPatternExecutionStep {
    private final List<String> relTypeNames;
    private final CypherDirection direction;
    private final String leftNodeName;
    private final String rightNodeName;

    public CreateRelationshipPatternExecutionStep(
        String name,
        List<String> relTypeNames,
        CypherDirection direction,
        String leftNodeName,
        String rightNodeName,
        List<ExecutionStepWithResultName> properties,
        List<ExecutionStep> mergeActions
    ) {
        super(ElementType.EDGE, name, properties, mergeActions);
        this.relTypeNames = relTypeNames;
        this.direction = direction;
        this.leftNodeName = leftNodeName;
        this.rightNodeName = rightNodeName;
    }

    public List<String> getRelTypeNames() {
        return relTypeNames;
    }

    public CypherDirection getDirection() {
        return direction;
    }

    @Override
    protected ElementMutation createElement(VertexiumCypherQueryContext ctx, CypherResultRow row) {
        Vertex left = (Vertex) row.get(leftNodeName);
        Vertex right = (Vertex) row.get(rightNodeName);

        Vertex outVertex = direction.hasOut() ? left : right;
        Vertex inVertex = direction.hasOut() ? right : left;

        String edgeId = ctx.calculateEdgeId(this, row);
        Visibility visibility = ctx.calculateEdgeVisibility(this, outVertex, inVertex, row);
        String label = ctx.calculateEdgeLabel(this, outVertex, inVertex, row);

        return ctx.getGraph().prepareEdge(edgeId, outVertex, inVertex, label, visibility);
    }

    @Override
    public String toString() {
        return String.format(
            "%s {relTypeNames=%s, direction=%s, leftNodeName='%s', rightNodeName='%s'}",
            super.toString(),
            String.join(", ", relTypeNames),
            direction,
            leftNodeName,
            rightNodeName
        );
    }
}
