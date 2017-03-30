package org.vertexium.cypher.executor;

import com.google.common.collect.ImmutableList;
import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.*;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryExecutor {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(QueryExecutor.class);

    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, CypherStatement statement) {
        LOGGER.debug("execute: %s", statement);
        checkNotNull(statement, "statement is required");
        CypherAstBase query = statement.getQuery();
        return executeQuery(ctx, query);
    }

    private VertexiumCypherScope executeQuery(VertexiumCypherQueryContext ctx, CypherAstBase query) {
        if (query instanceof CypherQuery) {
            return execute(ctx, (CypherQuery) query);
        } else if (query instanceof CypherUnion) {
            return executeUnion(ctx, (CypherUnion) query);
        }
        throw new VertexiumCypherTypeErrorException(query, CypherQuery.class, CypherUnion.class);
    }

    private VertexiumCypherScope executeUnion(VertexiumCypherQueryContext ctx, CypherUnion union) {
        VertexiumCypherScope leftResults = executeQuery(ctx, union.getLeft());
        VertexiumCypherScope rightResults = executeQuery(ctx, union.getRight());
        return leftResults.concat(rightResults, !union.isAll(), null);
    }

    private VertexiumCypherScope execute(VertexiumCypherQueryContext ctx, CypherQuery cypherQuery) {
        VertexiumCypherScope scope = VertexiumCypherScope.newSingleItemScope(VertexiumCypherScope.newEmptyItem());
        ImmutableList<CypherClause> clauses = cypherQuery.getClauses();
        AtomicInteger clauseIndex = new AtomicInteger(0);
        for (; clauseIndex.intValue() < clauses.size(); clauseIndex.incrementAndGet()) {
            scope = execute(ctx, clauses, clauseIndex, scope);
        }
        if (clauses.get(clauses.size() - 1) instanceof CypherReturnClause) {
            return scope;
        }
        return VertexiumCypherScope.newEmpty();
    }

    private VertexiumCypherScope execute(
            VertexiumCypherQueryContext ctx,
            ImmutableList<CypherClause> clauses,
            AtomicInteger clauseIndex,
            VertexiumCypherScope previousScope
    ) {
        VertexiumCypherScope results;
        CypherClause clause = clauses.get(clauseIndex.get());
        if (clause instanceof CypherCreateClause) {
            results = ctx.getCreateClauseExecutor().execute(ctx, (CypherCreateClause) clause, previousScope);
        } else if (clause instanceof CypherReturnClause) {
            results = ctx.getReturnClauseExecutor().execute(ctx, (CypherReturnClause) clause, previousScope);
        } else if (clause instanceof CypherMatchClause) {
            List<CypherMatchClause> matchClauses = getSimilarClauses(clauses, clauseIndex.get(), CypherMatchClause.class);
            results = ctx.getMatchClauseExecutor().execute(ctx, matchClauses, previousScope);
            clauseIndex.addAndGet(matchClauses.size() - 1);
        } else if (clause instanceof CypherUnwindClause) {
            List<CypherUnwindClause> unwindClauses = getSimilarClauses(clauses, clauseIndex.get(), CypherUnwindClause.class);
            results = ctx.getUnwindClauseExecutor().execute(ctx, unwindClauses, previousScope);
            clauseIndex.addAndGet(unwindClauses.size() - 1);
        } else if (clause instanceof CypherWithClause) {
            results = ctx.getWithClauseExecutor().execute(ctx, (CypherWithClause) clause, previousScope);
        } else if (clause instanceof CypherMergeClause) {
            results = ctx.getMergeClauseExecutor().execute(ctx, (CypherMergeClause) clause, previousScope);
        } else if (clause instanceof CypherDeleteClause) {
            results = ctx.getDeleteClauseExecutor().execute(ctx, (CypherDeleteClause) clause, previousScope);
        } else if (clause instanceof CypherSetClause) {
            results = ctx.getSetClauseExecutor().execute(ctx, (CypherSetClause) clause, previousScope);
        } else if (clause instanceof CypherRemoveClause) {
            results = ctx.getRemoveClauseExecutor().execute(ctx, (CypherRemoveClause) clause, previousScope);
        } else {
            throw new VertexiumException("clause not implemented \"" + clause.getClass().getName() + "\": " + clause);
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    private <T extends CypherClause> List<T> getSimilarClauses(ImmutableList<CypherClause> clauses, int clauseIndex, Class<T> clazz) {
        List<T> matchClauses = new ArrayList<T>();
        matchClauses.add((T) clauses.get(clauseIndex));
        while (clauseIndex + 1 < clauses.size() && clazz.isInstance(clauses.get(clauseIndex + 1))) {
            matchClauses.add((T) clauses.get(clauseIndex + 1));
            clauseIndex++;
        }
        return matchClauses;
    }
}
