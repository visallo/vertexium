package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.CypherResultRow;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.stream.Stream;

public class ListComprehensionExecutionStep extends ExecutionStepWithChildren implements ExecutionStepWithResultName {
    private final String resultName;
    private final String idInColVariableName;
    private final String idInColResultName;
    private final ExecutionStepWithResultName whereExpression;
    private final ExecutionStepWithResultName expression;

    // [x IN range(0,10) WHERE x % 2 = 0 | x^3]
    // [<idInColVariableName> IN <idInColExpression> WHERE <whereExpression> | <expression>]
    public ListComprehensionExecutionStep(
        String resultName,
        String idInColVariableName,
        ExecutionStepWithResultName idInColExpression,
        ExecutionStepWithResultName whereExpression,
        ExecutionStepWithResultName expression
    ) {
        super(idInColExpression);
        this.resultName = resultName;
        this.idInColVariableName = idInColVariableName;
        this.idInColResultName = idInColExpression.getResultName();
        this.whereExpression = whereExpression;
        this.expression = expression;
    }

    @Override
    public String getResultName() {
        return resultName;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);
        return source.peek(row -> {
            Object idInCol = row.get(idInColResultName);
            Stream<Object> idInColValue;
            if (idInCol instanceof Object[]) {
                idInColValue = Arrays.stream((Object[]) idInCol);
            } else if (idInCol instanceof Collection) {
                idInColValue = ((Collection) idInCol).stream();
            } else {
                throw new VertexiumCypherNotImplemented("Unhandled idInCol: " + idInCol + " (type: " + idInCol.getClass().getName() + ")");
            }
            Stream<CypherResultRow> idInColRows = idInColValue
                .map(v -> row.clone().set(idInColVariableName, v));

            VertexiumCypherResult data = new VertexiumCypherResult(idInColRows, new LinkedHashSet<>());
            if (whereExpression != null) {
                data = whereExpression.execute(ctx, data);
            }
            Object[] result = expression.execute(ctx, data)
                .map(r -> r.get(expression.getResultName()))
                .toArray(Object[]::new);
            row.pushScope(getResultName(), result);
        });
    }

    @Override
    public String toString() {
        return String.format(
            "%s {resultName='%s', idInColVariableName='%s', idInColResultName='%s', whereExpression=%s, expression=%s}",
            super.toString(),
            resultName,
            idInColVariableName,
            idInColResultName,
            whereExpression,
            expression
        );
    }
}
