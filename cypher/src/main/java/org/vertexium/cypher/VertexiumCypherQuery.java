package org.vertexium.cypher;

import org.vertexium.VertexiumException;
import org.vertexium.cypher.ast.CypherAstParser;
import org.vertexium.cypher.ast.CypherCompilerContext;
import org.vertexium.cypher.ast.model.CypherStatement;
import org.vertexium.cypher.executor.QueryExecutor;

import static com.google.common.base.Preconditions.checkNotNull;

public class VertexiumCypherQuery {
    private final CypherStatement statement;
    private final QueryExecutor queryExecutor;

    protected VertexiumCypherQuery(CypherStatement statement) {
        checkNotNull(statement, "statement is required");
        this.statement = statement;
        queryExecutor = new QueryExecutor();
    }

    public static VertexiumCypherQuery parse(CypherCompilerContext ctx, String queryString) {
        CypherStatement statement = CypherAstParser.getInstance().parse(ctx, queryString);
        if (statement == null) {
            throw new VertexiumException("Failed to parse query: " + queryString);
        }
        return new VertexiumCypherQuery(statement);
    }

    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx) {
        return queryExecutor.execute(ctx, statement);
    }
}
