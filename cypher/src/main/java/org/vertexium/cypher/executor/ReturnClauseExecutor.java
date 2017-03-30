package org.vertexium.cypher.executor;

import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.*;
import org.vertexium.cypher.functions.CypherFunction;
import org.vertexium.cypher.functions.aggregate.AggregationFunction;
import org.vertexium.cypher.utils.ObjectUtils;
import org.vertexium.util.StreamUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

public class ReturnClauseExecutor {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ReturnClauseExecutor.class);
    private final ExpressionExecutor expressionExecutor;

    public ReturnClauseExecutor(ExpressionExecutor expressionExecutor) {
        this.expressionExecutor = expressionExecutor;
    }

    public VertexiumCypherScope execute(VertexiumCypherQueryContext ctx, CypherReturnClause clause, VertexiumCypherScope scope) {
        LOGGER.debug("execute: %s", clause);
        return execute(ctx, clause.isDistinct(), clause.getReturnBody(), scope);
    }

    public VertexiumCypherScope execute(
            VertexiumCypherQueryContext ctx,
            boolean distinct,
            CypherReturnBody returnBody,
            VertexiumCypherScope scope
    ) {
        List<CypherReturnItem> returnItems = returnBody.getReturnItems()
                .stream()
                .flatMap(ri -> {
                    if (ri.getExpression() instanceof CypherAllLiteral) {
                        return getAllFieldNamesAsReturnItems(scope);
                    }
                    return Stream.of(ri);
                })
                .collect(Collectors.toList());
        LinkedHashSet<String> columnNames = getColumnNames(returnItems);

        Stream<VertexiumCypherScope.Item> rows = scope.stream();

        long aggregationCount = aggregationCount(ctx, returnItems);
        if (returnItems.size() > 0 && aggregationCount == returnItems.size()) {
            rows = Stream.of(getReturnRow(ctx, returnItems, null, scope));
        } else if (aggregationCount > 0 && isGroupable(returnItems.get(0))) {
            Map<Optional<?>, VertexiumCypherScope> groups = groupBy(ctx, returnItems.get(0), rows);
            rows = groups.entrySet().stream()
                    .map(group -> getReturnRow(ctx, returnItems, group.getKey(), group.getValue()));
        } else {
            rows = rows
                    .map(row -> getReturnRow(ctx, returnItems, null, row));
        }

        if (distinct) {
            rows = rows.distinct();
        }

        VertexiumCypherScope results = VertexiumCypherScope.newFromItems(rows, columnNames, scope);

        return applyReturnBody(ctx, returnBody, results);
    }

    private boolean isGroupable(CypherReturnItem cypherReturnItem) {
        CypherAstBase expression = cypherReturnItem.getExpression();
        if (expression instanceof CypherVariable
                || expression instanceof CypherLookup
                || expression instanceof CypherPatternComprehension) {
            return true;
        }
        if (expression instanceof CypherFunctionInvocation) {
            return false;
        }
        return false;
    }

    private VertexiumCypherScope.Item getReturnRow(
            VertexiumCypherQueryContext ctx,
            List<CypherReturnItem> returnItems,
            Optional<?> firstItemValue,
            ExpressionScope scope
    ) {
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < returnItems.size(); i++) {
            CypherReturnItem returnItem = returnItems.get(i);
            Object value;
            if (i == 0 && firstItemValue != null) {
                value = firstItemValue.orElse(null);
            } else {
                value = expressionExecutor.executeExpression(ctx, returnItem.getExpression(), scope);
            }
            value = expandResultMapSubItems(ctx, value, scope);
            values.put(returnItem.getResultColumnName(), value);
        }
        return VertexiumCypherScope.newMapItem(values, scope);
    }

    private LinkedHashSet<String> getColumnNames(Iterable<CypherReturnItem> returnItems) {
        return stream(returnItems)
                .map(CypherReturnItem::getResultColumnName)
                .collect(StreamUtils.toLinkedHashSet());
    }

    private Map<Optional<?>, VertexiumCypherScope> groupBy(
            VertexiumCypherQueryContext ctx,
            CypherReturnItem returnItem,
            Stream<VertexiumCypherScope.Item> rows
    ) {
        Set<Map.Entry<Optional<Object>, List<VertexiumCypherScope.Item>>> results = rows
                .collect(Collectors.groupingBy(row -> Optional.ofNullable(
                        ctx.getExpressionExecutor().executeExpression(ctx, returnItem.getExpression(), row)
                )))
                .entrySet();
        return results.stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        o -> {
                            List<VertexiumCypherScope.Item> items = o.getValue();
                            VertexiumCypherScope parentScope = items.get(0).getParentCypherScope();
                            return VertexiumCypherScope.newFromItems(items.stream(), parentScope);
                        }
                ));
    }

    private long aggregationCount(VertexiumCypherQueryContext ctx, Iterable<CypherReturnItem> returnItems) {
        return stream(returnItems)
                .filter(ri -> hasAggregations(ctx, ri))
                .count();
    }

    private boolean hasAggregations(VertexiumCypherQueryContext ctx, CypherAstBase ri) {
        if (ri == null) {
            return false;
        }
        if (ri instanceof CypherFunctionInvocation) {
            CypherFunction fn = ctx.getFunction(((CypherFunctionInvocation) ri).getFunctionName());
            if (fn != null && fn instanceof AggregationFunction) {
                return true;
            }
        }
        return ri.getChildren().anyMatch(child -> hasAggregations(ctx, child));
    }

    private Stream<CypherReturnItem> getAllFieldNamesAsReturnItems(VertexiumCypherScope scope) {
        return scope.getColumnNames().stream()
                .map(n -> new CypherReturnItem(n, new CypherVariable(n), n));
    }

    /*
     * This method will expand return items such as
     *
     * RETURN coalesce(a.prop, b.prop) AS foo,
     *        b.prop AS bar,
     *        {y: count(b)} AS baz
     *
     * In the above example {y: count(b)} in the map this method will expand
     */
    private Object expandResultMapSubItems(VertexiumCypherQueryContext ctx, Object value, ExpressionScope scope) {
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            Map<Object, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getValue() instanceof CypherAstBase) {
                    CypherAstBase entryValue = (CypherAstBase) entry.getValue();
                    Object newEntryValue = expressionExecutor.executeExpression(ctx, entryValue, scope);
                    newEntryValue = expandResultMapSubItems(ctx, newEntryValue, scope);
                    result.put(entry.getKey(), newEntryValue);
                } else {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
            return result;
        }
        return value;
    }

    public VertexiumCypherScope applyReturnBody(
            VertexiumCypherQueryContext ctx,
            CypherReturnBody returnBody,
            VertexiumCypherScope results
    ) {
        Stream<VertexiumCypherScope.Item> rows = results.stream();
        if (returnBody.getOrder() != null) {
            rows = applyOrderByToResults(ctx, rows, returnBody.getOrder());
        }
        if (returnBody.getSkip() != null) {
            rows = applySkipToResults(ctx, rows, returnBody.getSkip(), results);
        }
        if (returnBody.getLimit() != null) {
            rows = applyLimitToResults(ctx, rows, returnBody.getLimit(), results);
        }
        return VertexiumCypherScope.newFromItems(rows, results.getColumnNames(), results.getParentScope());
    }

    private Stream<VertexiumCypherScope.Item> applyOrderByToResults(
            VertexiumCypherQueryContext ctx,
            Stream<VertexiumCypherScope.Item> results,
            CypherOrderBy orderByClause
    ) {
        List<CypherSortItem> sortItems = orderByClause.getSortItems();
        return results.sorted((o1, o2) -> {
            for (CypherSortItem sortItem : sortItems) {
                Object v1 = getOrderByValue(ctx, sortItem, o1);
                Object v2 = getOrderByValue(ctx, sortItem, o2);
                int i = ObjectUtils.compare(v1, v2);
                if (i != 0) {
                    return i;
                }
            }
            return 0;
        });
    }

    private Object getOrderByValue(VertexiumCypherQueryContext ctx, CypherSortItem sortItem, VertexiumCypherScope.Item scope) {
        Object value = scope.getByName(sortItem.getExpression().toString(), false);
        if (value != null) {
            return value;
        }
        Object results = ctx.getExpressionExecutor().executeExpression(ctx, sortItem.getExpression(), scope);
        if (results instanceof Collection && ((Collection) results).size() > 0) {
            HashSet<?> resultsSet = new HashSet<>((Collection<?>) results);
            if (resultsSet.size() == 1) {
                for (Object item : resultsSet) {
                    return item;
                }
            }
        }
        return results;
    }

    private Stream<VertexiumCypherScope.Item> applyLimitToResults(
            VertexiumCypherQueryContext ctx,
            Stream<VertexiumCypherScope.Item> results,
            CypherLimit limitClause,
            VertexiumCypherScope scope
    ) {
        Object limitObj = ctx.getExpressionExecutor().executeExpression(ctx, limitClause.getExpression(), scope);
        int limit;
        if (limitObj instanceof Integer || limitObj instanceof Long) {
            limit = ((Number) limitObj).intValue();
        } else {
            throw new VertexiumException("limit with a none integer not supported: " + limitObj);
        }
        if (limit < 0) {
            limit = 0;
        }
        results = results.limit(limit);
        return results;
    }

    private Stream<VertexiumCypherScope.Item> applySkipToResults(
            VertexiumCypherQueryContext ctx,
            Stream<VertexiumCypherScope.Item> results,
            CypherSkip skipClause,
            VertexiumCypherScope scope
    ) {
        Object skipObj = ctx.getExpressionExecutor().executeExpression(ctx, skipClause.getExpression(), scope);
        int skip;
        if (skipObj instanceof Integer || skipObj instanceof Long) {
            skip = ((Number) skipObj).intValue();
        } else {
            throw new VertexiumException("skip with a none integer not supported: " + skipObj);
        }
        results = results.skip(skip);
        return results;
    }
}
