package org.vertexium.cypher.executionPlan;

import org.vertexium.Element;
import org.vertexium.ElementType;
import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.SingleRowVertexiumCypherResult;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.mutation.ElementMutation;

import java.util.List;
import java.util.stream.Collectors;

public abstract class CreateElementPatternExecutionStep<T extends Element> extends ExecutionStepWithChildren implements ExecutionStepWithResultName {
    private final ElementType elementType;
    private final String name;
    private final List<String> propertyResultNames;
    private final List<MergeActionExecutionStep> mergeActions;

    public CreateElementPatternExecutionStep(
        ElementType elementType,
        String name,
        List<ExecutionStepWithResultName> properties,
        List<MergeActionExecutionStep> mergeActions
    ) {
        super(properties.toArray(new ExecutionStepWithResultName[0]));
        this.elementType = elementType;
        this.name = name;
        this.propertyResultNames = properties.stream().map(ExecutionStepWithResultName::getResultName).collect(Collectors.toList());
        this.mergeActions = mergeActions;
    }

    @Override
    public String getResultName() {
        return name;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);

        return source.peek(row -> {
            if (row.get(getResultName()) != null) {
                for (MergeActionExecutionStep mergeAction : mergeActions) {
                    if (mergeAction.getType() == MergeActionExecutionStep.Type.MATCH) {
                        mergeAction.execute(ctx, new SingleRowVertexiumCypherResult(row)).count();
                    }
                }
                return;
            }

            ElementMutation<T> m = createElement(ctx, row);
            setProperties(ctx, row, m);
            row.set(getResultName(), ctx.saveElement(elementType, m));

            for (MergeActionExecutionStep mergeAction : mergeActions) {
                if (mergeAction.getType() == MergeActionExecutionStep.Type.CREATE) {
                    mergeAction.execute(ctx, new SingleRowVertexiumCypherResult(row)).count();
                }
            }
        });
    }

    protected abstract ElementMutation<T> createElement(VertexiumCypherQueryContext ctx, CypherResultRow row);

    protected void setProperties(VertexiumCypherQueryContext ctx, CypherResultRow row, ElementMutation<T> m) {
        for (String propertyResultName : propertyResultNames) {
            String propertyName = ctx.normalizePropertyName(propertyResultName);
            Object value = row.get(propertyResultName);
            if (value instanceof CypherAstBase) {
                throw new VertexiumCypherNotImplemented("Unhandled type: " + value.getClass().getName());
            }
            if (value != null) {
                ctx.setProperty(m, propertyName, value);
            }
        }
    }

    @Override
    public String toString() {
        return String.format(
            "%s {name='%s', propertyResultNames=%s}",
            super.toString(),
            name,
            String.join(", ", propertyResultNames)
        );
    }
}
