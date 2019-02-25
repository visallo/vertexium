package org.vertexium.cypher.executor;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.CypherMergeActionCreate;
import org.vertexium.cypher.ast.model.CypherMergeActionMatch;
import org.vertexium.cypher.ast.model.CypherMergeClause;
import org.vertexium.cypher.executor.models.match.PatternPartMatchConstraint;
import org.vertexium.cypher.executor.utils.MatchConstraintBuilder;
import org.vertexium.util.StreamUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.LinkedHashMap;
import java.util.stream.Stream;

public class MergeClauseExecutor {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(MergeClauseExecutor.class);

    public VertexiumCypherScope execute(VertexiumCypherQueryContext ctx, CypherMergeClause clause, VertexiumCypherScope scope) {
        LOGGER.debug("execute: %s", clause);
        scope.run(); // need to materialize the scope
        PatternPartMatchConstraint patternPartConstraint = new MatchConstraintBuilder().patternPartToConstraints(clause.getPatternPart(), false);
        Stream<VertexiumCypherScope> results = scope.stream().map(item -> {
            Stream<VertexiumCypherScope.Item> patternPartResults = ctx.getMatchClauseExecutor().executePatternPartConstraint(ctx, patternPartConstraint, item);
            return StreamUtils.ifEmpty(
                patternPartResults,
                () -> {
                    Stream<VertexiumCypherScope.Item> createResults = executeCreate(ctx, clause, patternPartConstraint, item);
                    return VertexiumCypherScope.newItemsScope(createResults, scope);
                },
                (stream) -> executeMatch(ctx, clause, stream, scope)
            );
        });
        return results.collect(VertexiumCypherScope.concatStreams(scope));
    }

    private VertexiumCypherScope executeMatch(
        VertexiumCypherQueryContext ctx,
        CypherMergeClause clause,
        Stream<VertexiumCypherScope.Item> results,
        VertexiumCypherScope scope
    ) {
        results = results.map(result -> {
            clause.getMergeActions().stream()
                .filter(ma -> ma instanceof CypherMergeActionMatch)
                .forEach(ma -> executeMatchMergeAction(ctx, (CypherMergeActionMatch) ma, result));
            return result;
        });
        return VertexiumCypherScope.newItemsScope(results, scope);
    }

    private void executeMatchMergeAction(
        VertexiumCypherQueryContext ctx,
        CypherMergeActionMatch ma,
        VertexiumCypherScope.Item existingObj
    ) {
        ctx.getSetClauseExecutor().execute(ctx, ma.getSet(), existingObj);
    }

    private Stream<VertexiumCypherScope.Item> executeCreate(
        VertexiumCypherQueryContext ctx,
        CypherMergeClause clause,
        PatternPartMatchConstraint patternPartConstraint,
        VertexiumCypherScope.Item item
    ) {
        LinkedHashMap<String, ?> map = ctx.getCreateClauseExecutor().executePatternPart(ctx, clause.getPatternPart(), item);
        VertexiumCypherScope.Item concatItem = item.concat(VertexiumCypherScope.newMapItem(map, item.getParentScope()));
        clause.getMergeActions().stream()
            .filter(ma -> ma instanceof CypherMergeActionCreate)
            .forEach(ma -> executeCreateMergeAction(ctx, (CypherMergeActionCreate) ma, concatItem));

        return ctx.getMatchClauseExecutor().executePatternPartConstraint(ctx, patternPartConstraint, item);
    }

    private void executeCreateMergeAction(
        VertexiumCypherQueryContext ctx,
        CypherMergeActionCreate ma,
        VertexiumCypherScope.Item newObj
    ) {
        ctx.getSetClauseExecutor().execute(ctx, ma.getSet(), newObj);
    }
}
