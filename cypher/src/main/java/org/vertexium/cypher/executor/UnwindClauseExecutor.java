package org.vertexium.cypher.executor;

import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.CypherUnwindClause;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UnwindClauseExecutor {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(UnwindClauseExecutor.class);
    private final ExpressionExecutor expressionExecutor;

    public UnwindClauseExecutor(ExpressionExecutor expressionExecutor) {
        this.expressionExecutor = expressionExecutor;
    }

    public VertexiumCypherScope execute(VertexiumCypherQueryContext ctx, List<CypherUnwindClause> clauses, VertexiumCypherScope scope) {
        List<VertexiumCypherScope> allResults = new ArrayList<>();
        VertexiumCypherScope accumulatorScope = scope;
        for (CypherUnwindClause clause : clauses) {
            List<VertexiumCypherScope.Item> clauseResults = new ArrayList<>();
            accumulatorScope.stream().forEach(item -> {
                List<VertexiumCypherScope.Item> items = execute(ctx, clause, item).collect(Collectors.toList());
                clauseResults.addAll(items);
            });
            accumulatorScope = VertexiumCypherScope.newItemsScope(clauseResults, scope);
            allResults.add(accumulatorScope);
        }

        return accumulatorScope;
    }

    private Stream<VertexiumCypherScope.Item> execute(VertexiumCypherQueryContext ctx, CypherUnwindClause clause, ExpressionScope scope) {
        LOGGER.debug("execute: %s", clause);
        Object exprResult = expressionExecutor.executeExpression(ctx, clause.getExpression(), scope);
        Stream<?> stream;
        if (exprResult == null) {
            return Stream.of();
        } else if (exprResult instanceof Stream) {
            stream = (Stream<?>) exprResult;
        } else if (exprResult instanceof Collection) {
            Collection<?> collection = (Collection<?>) exprResult;
            stream = collection.stream();
        } else {
            throw new VertexiumException("unhandled data type: " + exprResult.getClass().getName());
        }
        return stream
                .map(o -> VertexiumCypherScope.newMapItem(clause.getName(), o, scope));
    }
}
