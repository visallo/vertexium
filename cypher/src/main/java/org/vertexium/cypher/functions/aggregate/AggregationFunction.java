package org.vertexium.cypher.functions.aggregate;

import org.vertexium.cypher.ast.CypherCompilerContext;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.ast.model.CypherFunctionInvocation;
import org.vertexium.cypher.exceptions.VertexiumCypherSyntaxErrorException;
import org.vertexium.cypher.functions.CypherFunction;

public abstract class AggregationFunction extends CypherFunction {
    @Override
    public void compile(CypherCompilerContext ctx, CypherAstBase[] arguments) {
        for (CypherAstBase argument : arguments) {
            checkChildren(ctx, argument);
        }
    }

    private void checkChildren(CypherCompilerContext ctx, CypherAstBase argument) {
        if (argument instanceof CypherFunctionInvocation) {
            CypherFunction fn = ctx.getFunction(((CypherFunctionInvocation) argument).getFunctionName());
            if (fn != null) {
                if (fn instanceof AggregationFunction) {
                    throw new VertexiumCypherSyntaxErrorException("NestedAggregation: Aggregation functions cannot have aggregations as arguments.");
                }
                if (!fn.isConstant()) {
                    throw new VertexiumCypherSyntaxErrorException("NonConstantExpression: Aggregation functions cannot have non-constant values as arguments.");
                }
            }
        }
        argument.getChildren().forEach(child -> checkChildren(ctx, child));
    }
}
