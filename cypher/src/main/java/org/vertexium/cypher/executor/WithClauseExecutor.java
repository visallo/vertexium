package org.vertexium.cypher.executor;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.CypherWithClause;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WithClauseExecutor {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(WithClauseExecutor.class);

    public VertexiumCypherScope execute(VertexiumCypherQueryContext ctx, CypherWithClause clause, VertexiumCypherScope scope) {
        LOGGER.debug("execute: %s", clause);
        VertexiumCypherScope results = ctx.getReturnClauseExecutor().execute(ctx, clause.isDistinct(), clause.getReturnBody(), scope);
        Stream<VertexiumCypherScope.Item> rows = results.stream();

        if (clause.getWhere() != null) {
            rows = ctx.getExpressionExecutor().applyWhereToResults(ctx, rows, clause.getWhere());
        }

        List<VertexiumCypherScope.Item> rowsList = rows.collect(Collectors.toList()); // TODO remove need to materialize where
        return VertexiumCypherScope.newItemsScope(rowsList, results.getColumnNames(), null);
    }
}
