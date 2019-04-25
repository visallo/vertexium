package org.vertexium.cypher;

import org.vertexium.VertexiumException;
import org.vertexium.cypher.ast.CypherAstParser;
import org.vertexium.cypher.ast.CypherCompilerContext;
import org.vertexium.cypher.ast.model.CypherStatement;
import org.vertexium.cypher.executionPlan.ExecutionPlan;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class VertexiumCypherQuery {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(VertexiumCypherQuery.class);
    private final CypherStatement statement;

    protected VertexiumCypherQuery(CypherStatement statement) {
        checkNotNull(statement, "statement is required");
        this.statement = statement;
    }

    public static VertexiumCypherQuery parse(CypherCompilerContext ctx, String queryString) {
        CypherStatement statement = CypherAstParser.getInstance().parse(ctx, queryString);
        if (statement == null) {
            throw new VertexiumException("Failed to parse query: " + queryString);
        }
        return new VertexiumCypherQuery(statement);
    }

    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx) {
        LOGGER.info("Executing:\n%s", statement);
        ExecutionPlan plan = ctx.getExecutionPlanBuilder().build(ctx, statement);
        LOGGER.info("Execution plan:\n%s", plan.toStringFull());
        return plan.execute(ctx);
    }
}
