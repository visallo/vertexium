package org.vertexium.cypher.executor;

import com.google.common.collect.Lists;
import org.vertexium.Element;
import org.vertexium.Vertex;
import org.vertexium.VertexiumException;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherScope;
import org.vertexium.cypher.ast.model.*;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.exceptions.VertexiumCypherTypeErrorException;
import org.vertexium.cypher.functions.CypherFunction;
import org.vertexium.cypher.utils.MapUtils;
import org.vertexium.cypher.utils.ObjectUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

public class ExpressionExecutor {
    public Object executeExpression(VertexiumCypherQueryContext ctx, CypherAstBase expression, ExpressionScope scope) {
        if (expression == null) {
            return null;
        }

        if (expression instanceof CypherExpression) {
            if (expression instanceof CypherBinaryExpression) {
                return executeBinaryExpression(ctx, (CypherBinaryExpression) expression, scope);
            } else if (expression instanceof CypherComparisonExpression) {
                return executeComparisonExpression(ctx, (CypherComparisonExpression) expression, scope);
            } else if (expression instanceof CypherUnaryExpression) {
                return executeUnaryExpression(ctx, (CypherUnaryExpression) expression, scope);
            } else if (expression instanceof CypherTrueExpression) {
                return true;
            } else if (expression instanceof CypherNegateExpression) {
                return executeNegateExpression(ctx, (CypherNegateExpression) expression, scope);
            }
            throw new VertexiumCypherNotImplemented("" + expression);
        }

        if (expression instanceof CypherListLiteral) {
            //noinspection unchecked
            CypherListLiteral<? extends CypherAstBase> list = (CypherListLiteral<? extends CypherAstBase>) expression;
            return executeList(ctx, list, scope);
        }

        if (expression instanceof CypherLiteral) {
            CypherLiteral literal = (CypherLiteral) expression;
            return literal.getValue();
        }

        if (expression instanceof CypherVariable) {
            CypherVariable variable = (CypherVariable) expression;
            return executeObject(ctx, executeVariable(ctx, variable, scope), scope);
        }

        if (expression instanceof CypherLookup) {
            CypherLookup lookup = (CypherLookup) expression;
            return executeLookup(ctx, lookup, scope);
        }

        if (expression instanceof CypherFunctionInvocation) {
            CypherFunctionInvocation functionInvocation = (CypherFunctionInvocation) expression;
            return executeFunctionInvocation(ctx, functionInvocation, scope);
        }

        if (expression instanceof CypherIn) {
            CypherIn in = (CypherIn) expression;
            return executeIn(ctx, in, scope);
        }

        if (expression instanceof CypherArrayAccess) {
            CypherArrayAccess arrayAccess = (CypherArrayAccess) expression;
            return executeArrayAccess(ctx, arrayAccess, scope);
        }

        if (expression instanceof CypherArraySlice) {
            CypherArraySlice arraySlice = (CypherArraySlice) expression;
            return executeArraySlice(ctx, arraySlice, scope);
        }

        if (expression instanceof CypherParameter) {
            CypherParameter parameter = (CypherParameter) expression;
            return executeParameter(ctx, parameter);
        }

        if (expression instanceof CypherIsNull) {
            CypherIsNull isNull = (CypherIsNull) expression;
            return executeIsNull(ctx, isNull, scope);
        }

        if (expression instanceof CypherIsNotNull) {
            CypherIsNotNull isNotNull = (CypherIsNotNull) expression;
            return executeIsNotNull(ctx, isNotNull, scope);
        }

        if (expression instanceof CypherListComprehension) {
            CypherListComprehension listComprehension = (CypherListComprehension) expression;
            return executeListComprehension(ctx, listComprehension, scope);
        }

        if (expression instanceof CypherStringMatch) {
            CypherStringMatch startWith = (CypherStringMatch) expression;
            return executeStringMatch(ctx, startWith, scope);
        }

        if (expression instanceof CypherPatternComprehension) {
            CypherPatternComprehension patternComprehension = (CypherPatternComprehension) expression;
            VertexiumCypherScope matchScope = scope instanceof VertexiumCypherScope
                    ? (VertexiumCypherScope) scope
                    : VertexiumCypherScope.newSingleItemScope((VertexiumCypherScope.Item) scope);
            VertexiumCypherScope results = ctx.getMatchClauseExecutor().execute(
                    ctx,
                    Lists.newArrayList(patternComprehension.getMatchClause()),
                    matchScope
            );
            return results.stream()
                    .map(item -> executeExpression(ctx, patternComprehension.getExpression(), item))
                    .collect(Collectors.toList());
        }

        throw new VertexiumException("not implemented \"" + expression.getClass().getName() + "\": " + expression);
    }

    private Object executeNegateExpression(VertexiumCypherQueryContext ctx, CypherNegateExpression expression, ExpressionScope scope) {
        Object value = executeExpression(ctx, expression.getValue(), scope);
        if (value instanceof Number) {
            if (value instanceof Double) {
                return -(Double) value;
            }
            if (value instanceof Integer) {
                return -(Integer) value;
            }
            if (value instanceof Long) {
                return -(Long) value;
            }
            return -((Number) value).doubleValue();
        }
        throw new VertexiumException("not implemented");
    }

    private Object executeStringMatch(VertexiumCypherQueryContext ctx, CypherStringMatch stringMatch, ExpressionScope scope) {
        Object value = executeExpression(ctx, stringMatch.getValueExpression(), scope);
        Object stringObj = executeExpression(ctx, stringMatch.getStringExpression(), scope);
        if (stringObj == null) {
            return null;
        }
        if (!(stringObj instanceof String)) {
            return null;
        }
        String string = (String) stringObj;
        if (value == null) {
            return null;
        }
        switch (stringMatch.getOp()) {
            case STARTS_WITH:
                return value.toString().startsWith(string);
            case ENDS_WITH:
                return value.toString().endsWith(string);
            case CONTAINS:
                return value.toString().contains(string);
            default:
                throw new VertexiumException("unhandled string match: " + stringMatch.getOp());
        }
    }

    private Object executeListComprehension(VertexiumCypherQueryContext ctx, CypherListComprehension listComprehension, ExpressionScope scope) {
        Stream<ExpressionScope> itemScopes = executeFilterExpression(ctx, listComprehension.getFilterExpression(), scope);
        if (listComprehension.getExpression() == null) {
            return itemScopes;
        }
        return itemScopes
                .map(itemScope -> executeExpression(ctx, listComprehension.getExpression(), itemScope))
                .collect(Collectors.toList());
    }

    private Stream<ExpressionScope> executeFilterExpression(
            VertexiumCypherQueryContext ctx,
            CypherFilterExpression filterExpression,
            ExpressionScope scope
    ) {
        String name = filterExpression.getIdInCol().getVariable().getName();
        Object values = executeExpression(ctx, filterExpression.getIdInCol().getExpression(), scope);
        Stream<ExpressionScope> results = stream(toIterable(values))
                .map(value -> VertexiumCypherScope.newMapItem(name, value, scope));

        if (filterExpression.getWhere() != null) {
            throw new VertexiumCypherNotImplemented("where");
        }

        return results;
    }

    private Object executeObject(VertexiumCypherQueryContext ctx, Object o, ExpressionScope scope) {
        if (o instanceof CypherAstBase) {
            return executeExpression(ctx, (CypherAstBase) o, scope);
        }
        return o;
    }

    private Object executeIsNotNull(VertexiumCypherQueryContext ctx, CypherIsNotNull isNotNull, ExpressionScope clauseResult) {
        Object value = ctx.getExpressionExecutor().executeExpression(ctx, isNotNull.getValueExpression(), clauseResult);
        return value != null;
    }

    private Object executeIsNull(VertexiumCypherQueryContext ctx, CypherIsNull isNull, ExpressionScope scope) {
        Object value = ctx.getExpressionExecutor().executeExpression(ctx, isNull.getValueExpression(), scope);
        return value == null;
    }

    private Object executeParameter(VertexiumCypherQueryContext ctx, CypherParameter parameter) {
        if (parameter instanceof CypherNameParameter) {
            CypherNameParameter nameParameter = (CypherNameParameter) parameter;
            return ctx.getParameters().get(nameParameter.getParameterName());
        } else if (parameter instanceof CypherIndexedParameter) {
            CypherIndexedParameter indexedParameter = (CypherIndexedParameter) parameter;
            return ctx.getParameters().get(Integer.toString(indexedParameter.getIndex()));
        }
        throw new VertexiumException("not implemented");
    }

    private Object executeArraySlice(VertexiumCypherQueryContext ctx, CypherArraySlice arraySlice, ExpressionScope scope) {
        Object array = ctx.getExpressionExecutor().executeExpression(ctx, arraySlice.getArrayExpression(), scope);
        Object sliceFromObj = ctx.getExpressionExecutor().executeExpression(ctx, arraySlice.getSliceFrom(), scope);
        Object sliceToObj = ctx.getExpressionExecutor().executeExpression(ctx, arraySlice.getSliceTo(), scope);

        if (!(sliceFromObj instanceof Number)) {
            throw new VertexiumException("expected integer from, found " + sliceFromObj.getClass().getName());
        }
        int sliceFrom = ((Number) sliceFromObj).intValue();

        if (!(sliceToObj instanceof Number)) {
            throw new VertexiumException("expected integer to, found " + sliceToObj.getClass().getName());
        }
        int sliceTo = ((Number) sliceToObj).intValue();

        Iterable<?> it = toIterable(array);
        return stream(it)
                .skip(sliceFrom)
                .limit(sliceTo - sliceFrom)
                .collect(Collectors.toList());
    }

    private Object executeArrayAccess(VertexiumCypherQueryContext ctx, CypherArrayAccess arrayAccess, ExpressionScope scope) {
        Object array = ctx.getExpressionExecutor().executeExpression(ctx, arrayAccess.getArrayExpression(), scope);
        if (array == null) {
            return null;
        }
        Object indexObj = ctx.getExpressionExecutor().executeExpression(ctx, arrayAccess.getIndexExpression(), scope);

        if (array instanceof Element) {
            Element element = (Element) array;
            if (indexObj instanceof String) {
                String propertyName = (String) indexObj;
                return element.getPropertyValue(propertyName);
            }

            throw new VertexiumCypherTypeErrorException("expected string property name, found " + indexObj.getClass().getName());
        }

        if (array instanceof Map) {
            Map map = (Map) array;
            if (indexObj instanceof String) {
                String propertyName = (String) indexObj;
                return map.get(propertyName);
            }

            throw new VertexiumCypherTypeErrorException("MapElementAccessByNonString: expected string, found " + indexObj.getClass().getName());
        }

        if (array instanceof List || array instanceof CypherListLiteral) {
            if (indexObj instanceof Long) {
                indexObj = ((Long) indexObj).intValue();
            }

            if (indexObj instanceof Integer) {
                int index = (int) indexObj;
                if (array instanceof CypherListLiteral) {
                    return ((CypherListLiteral) array).get(index);
                } else if (array instanceof List) {
                    return ((List) array).get(index);
                }
            }

            throw new VertexiumCypherTypeErrorException("ListElementAccessByNonInteger: expected integer, found " + indexObj.getClass().getName());
        }

        throw new VertexiumCypherTypeErrorException("InvalidElementAccess: unexpected object access, found object " + array.getClass().getName() + ": " + array + ", index " + indexObj.getClass().getName() + ": " + indexObj);
    }

    private Object executeIn(VertexiumCypherQueryContext ctx, CypherIn in, ExpressionScope scope) {
        Object value = ctx.getExpressionExecutor().executeExpression(ctx, in.getValueExpression(), scope);
        Object array = ctx.getExpressionExecutor().executeExpression(ctx, in.getArrayExpression(), scope);
        if (value == null) {
            if (array == null) {
                return null;
            }
            Iterable<?> it = toIterable(array);
            if (!it.iterator().hasNext()) {
                return false;
            }
            return null;
        }
        Iterable<?> it = toIterable(array);

        boolean hasNullValue = false;
        for (Object o : it) {
            if (o == null) {
                hasNullValue = true;
                continue;
            }
            if (o instanceof CypherLiteral) {
                o = ((CypherLiteral) o).getValue();
            }
            if (o.equals(value)) {
                return true;
            }
        }
        if (hasNullValue) {
            return null;
        }
        return false;
    }

    private Iterable<?> toIterable(Object obj) {
        Iterable<?> it;
        if (obj instanceof CypherListLiteral || obj instanceof List || obj instanceof Set) {
            it = (Iterable) obj;
        } else {
            throw new VertexiumCypherNotImplemented("expected iterable, found " + obj == null ? null : obj.getClass().getName());
        }
        return it;
    }

    private Object executeUnaryExpression(
            VertexiumCypherQueryContext ctx,
            CypherUnaryExpression expression,
            ExpressionScope scope
    ) {
        Object value = ctx.getExpressionExecutor().executeExpression(ctx, expression.getExpression(), scope);
        switch (expression.getOp()) {
            case NOT:
                return executeNOT(value);
            default:
                throw new VertexiumCypherNotImplemented("" + expression.getOp());
        }
    }

    private Object executeNOT(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return !((boolean) value);
        }
        throw new VertexiumException("could not NOT: " + value.getClass().getName());
    }

    private Object executeFunctionInvocation(
            VertexiumCypherQueryContext ctx,
            CypherFunctionInvocation functionInvocation,
            ExpressionScope scope
    ) {
        CypherFunction fn = ctx.getFunction(functionInvocation.getFunctionName());
        return fn.invoke(ctx, functionInvocation.getArguments(), scope);
    }

    private List<Object> executeList(VertexiumCypherQueryContext context, CypherListLiteral<? extends CypherAstBase> list, ExpressionScope scope) {
        return list.stream()
                .map(i -> executeExpression(context, i, scope))
                .collect(Collectors.toList());
    }

    private Object executeLookup(VertexiumCypherQueryContext ctx, CypherLookup expression, ExpressionScope scope) {
        Object item = executeExpression(ctx, expression.getAtom(), scope);
        return executeLookup(ctx, item, expression, scope);
    }

    private Object executeLookup(VertexiumCypherQueryContext ctx, Object item, CypherLookup expression, ExpressionScope scope) {
        if (item == null) {
            return null;
        }

        if (item instanceof Map) {
            if (expression.hasLabels()) {
                throw new VertexiumException("lookup using labels from a map is not supported");
            }
            Object value = MapUtils.getByExpression((Map) item, expression.getProperty());
            if (value == null) {
                return null;
            }
            if (value instanceof Element) {
                return value;
            }
            if (value instanceof CypherAstBase) {
                return executeExpression(ctx, (CypherAstBase) value, scope);
            }
            return value;
        }

        if (item instanceof Element) {
            Element element = (Element) item;

            if (expression.hasLabels()) {
                if (element instanceof Vertex) {
                    if (expression.getProperty() != null) {
                        throw new VertexiumException("cannot have labels and properties");
                    }
                    return expression.getLabels().stream()
                            .anyMatch(l -> ctx.getVertexLabels((Vertex) element)
                                    .contains(ctx.normalizeLabelName(l.getValue()))
                            );
                }
                throw new VertexiumCypherNotImplemented("label lookup");
            }

            if (expression.getProperty() == null) {
                return element;
            }

            return element.getPropertyValue(ctx.normalizePropertyName(expression.getProperty()));
        }

        if (item instanceof Collection) {
            Collection<?> list = (Collection) item;
            return list.stream()
                    .map(listItem -> {
                        Object results = executeLookup(ctx, listItem, expression, scope);
                        return results;
                    })
                    .collect(Collectors.toList());
        }

        throw new VertexiumCypherTypeErrorException(item, Element.class, Map.class, Collection.class, null);
    }

    private Object executeComparisonExpression(
            VertexiumCypherQueryContext context,
            CypherComparisonExpression expression,
            ExpressionScope scope
    ) {
        Object left = executeExpression(context, expression.getLeft(), scope);
        Object right = executeExpression(context, expression.getRight(), scope);
        String op = expression.getOp();
        switch (op) {
            case "=":
            case "<":
            case "<=":
            case ">":
            case ">=":
            case "<>":
                if (left == null && right == null) {
                    return null;
                }
                if (left == null) {
                    return false;
                }
                return compare(left, op, right);
            default:
                throw new VertexiumException("comparison not implemented: " + op);
        }
    }

    private Object compare(Object left, String op, Object right) {
        switch (op) {
            case "<":
            case "<=":
            case ">":
            case ">=":
                if (left == null || right == null) {
                    return false;
                }
                if (left instanceof Number && right instanceof Number) {
                    // numbers are ok
                } else if (left.getClass().equals(right.getClass())) {
                    // same types are ok
                } else {
                    return false;
                }
        }

        int comp = ObjectUtils.compare(left, right);
        switch (op) {
            case "=":
                return comp == 0;
            case "<":
                return comp < 0;
            case "<=":
                return comp <= 0;
            case ">":
                return comp > 0;
            case ">=":
                return comp >= 0;
            case "<>":
                return comp != 0;
            default:
                throw new VertexiumCypherNotImplemented("unexpected op: " + op);
        }
    }

    private Object executeBinaryExpression(
            VertexiumCypherQueryContext ctx,
            CypherBinaryExpression expression,
            ExpressionScope scope
    ) {
        Object left = executeExpression(ctx, expression.getLeft(), scope);
        switch (expression.getOp()) {
            case ADD:
                return executeADD(ctx, left, expression.getRight(), scope);
            case MULTIPLY:
                return executeMULTIPLY(ctx, left, expression.getRight(), scope);
            case MINUS:
                return executeMINUS(ctx, left, expression.getRight(), scope);
            case DIVIDE:
                return executeDIVIDE(ctx, left, expression.getRight(), scope);
            case AND:
                return executeAND(ctx, left, expression.getRight(), scope);
            case OR:
                return executeOR(ctx, left, expression.getRight(), scope);
            case XOR:
                return executeXOR(ctx, left, expression.getRight(), scope);
            case MOD:
                return executeMOD(ctx, left, expression.getRight(), scope);
            case POWER:
                return executePOWER(ctx, left, expression.getRight(), scope);
            default:
                throw new VertexiumException("Unhandled binary op: " + expression.getOp());
        }
    }

    private Object executeMULTIPLY(VertexiumCypherQueryContext ctx, Object left, CypherAstBase rightExpression, ExpressionScope scope) {
        Object right = executeExpression(ctx, rightExpression, scope);
        if (right == null) {
            return null;
        }

        if (!(left instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(left, Number.class);
        }
        if (!(right instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(right, Number.class);
        }

        Number leftNumber = (Number) left;
        Number rightNumber = (Number) right;
        if (leftNumber instanceof Double || leftNumber instanceof Float
                || rightNumber instanceof Double || rightNumber instanceof Float) {
            return leftNumber.doubleValue() * rightNumber.doubleValue();
        }
        if (leftNumber instanceof Long || rightNumber instanceof Long) {
            return leftNumber.longValue() * rightNumber.longValue();
        }
        return leftNumber.intValue() * rightNumber.intValue();
    }

    private Object executeMINUS(VertexiumCypherQueryContext ctx, Object left, CypherAstBase rightExpression, ExpressionScope scope) {
        Object right = executeExpression(ctx, rightExpression, scope);
        if (right == null) {
            return null;
        }

        if (!(left instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(left, Number.class);
        }
        if (!(right instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(right, Number.class);
        }

        Number leftNumber = (Number) left;
        Number rightNumber = (Number) right;
        if (leftNumber instanceof Double || leftNumber instanceof Float
                || rightNumber instanceof Double || rightNumber instanceof Float) {
            return leftNumber.doubleValue() - rightNumber.doubleValue();
        }
        if (leftNumber instanceof Long || rightNumber instanceof Long) {
            return leftNumber.longValue() - rightNumber.longValue();
        }
        return leftNumber.intValue() - rightNumber.intValue();
    }

    private Object executeDIVIDE(VertexiumCypherQueryContext ctx, Object left, CypherAstBase rightExpression, ExpressionScope scope) {
        Object right = executeExpression(ctx, rightExpression, scope);
        if (right == null) {
            return null;
        }

        if (!(left instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(left, Number.class);
        }
        if (!(right instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(right, Number.class);
        }

        Number leftNumber = (Number) left;
        Number rightNumber = (Number) right;
        if (leftNumber instanceof Double || leftNumber instanceof Float
                || rightNumber instanceof Double || rightNumber instanceof Float) {
            return leftNumber.doubleValue() / rightNumber.doubleValue();
        }
        if (leftNumber instanceof Long || rightNumber instanceof Long) {
            return leftNumber.longValue() / rightNumber.longValue();
        }
        return leftNumber.intValue() / rightNumber.intValue();
    }

    private Object executeMOD(VertexiumCypherQueryContext ctx, Object left, CypherAstBase rightExpression, ExpressionScope scope) {
        Object right = executeExpression(ctx, rightExpression, scope);
        if (right == null) {
            return null;
        }

        if (!(left instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(left, Number.class);
        }
        if (!(right instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(right, Number.class);
        }

        Number leftNumber = (Number) left;
        Number rightNumber = (Number) right;
        if (leftNumber instanceof Double || leftNumber instanceof Float
                || rightNumber instanceof Double || rightNumber instanceof Float) {
            return leftNumber.doubleValue() % rightNumber.doubleValue();
        }
        if (leftNumber instanceof Long || rightNumber instanceof Long) {
            return leftNumber.longValue() % rightNumber.longValue();
        }
        return leftNumber.intValue() % rightNumber.intValue();
    }

    private Object executePOWER(VertexiumCypherQueryContext ctx, Object left, CypherAstBase rightExpression, ExpressionScope scope) {
        Object right = executeExpression(ctx, rightExpression, scope);
        if (right == null) {
            return null;
        }

        if (!(left instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(left, Number.class);
        }
        if (!(right instanceof Number)) {
            throw new VertexiumCypherTypeErrorException(right, Number.class);
        }

        Number leftNumber = (Number) left;
        Number rightNumber = (Number) right;
        if (leftNumber instanceof Double || leftNumber instanceof Float
                || rightNumber instanceof Double || rightNumber instanceof Float) {
            return Math.pow(leftNumber.doubleValue(), rightNumber.doubleValue());
        }
        if (leftNumber instanceof Long || rightNumber instanceof Long) {
            return (long) Math.pow(leftNumber.longValue(), rightNumber.longValue());
        }
        return (long) Math.pow(leftNumber.intValue(), rightNumber.intValue());
    }

    private Object executeADD(VertexiumCypherQueryContext ctx, Object left, CypherAstBase rightExpression, ExpressionScope scope) {
        Object right = executeExpression(ctx, rightExpression, scope);

        if (left instanceof String) {
            return ((String) left) + right;
        }
        if (left instanceof Number && right instanceof Number) {
            return ObjectUtils.addNumbers((Number) left, (Number) right);
        }

        if (left instanceof List && right instanceof List) {
            ArrayList<Object> results = new ArrayList<>();
            results.addAll((List) left);
            results.addAll((List) right);
            return results;
        }

        if (left instanceof List) {
            ArrayList<Object> results = new ArrayList<>();
            results.addAll((List) left);
            results.add(right);
            return results;
        }

        if (right == null) {
            return null;
        }

        throw new VertexiumException("add not implemented left:" + left.getClass().getName() + ", right:" + right.getClass().getName());
    }

    private Object executeAND(VertexiumCypherQueryContext ctx, Object left, CypherAstBase rightExpression, ExpressionScope scope) {
        if (left == null) {
            Object right = executeExpression(ctx, rightExpression, scope);
            if (right == null) {
                return null;
            }
            if (right instanceof Boolean) {
                boolean b = (boolean) right;
                if (b) {
                    return null;
                } else {
                    return false;
                }
            }
        }

        if (left instanceof Boolean) {
            boolean bLeft = (boolean) left;
            if (!bLeft) {
                return false;
            }
            Object right = executeExpression(ctx, rightExpression, scope);
            if (right == null) {
                return null;
            }
            if (right instanceof Boolean) {
                return right;
            }
            throw new VertexiumException("unexpected value in AND expression: " + right);
        }

        throw new VertexiumException("unexpected value in AND expression: " + left);
    }

    private Object executeOR(VertexiumCypherQueryContext ctx, Object left, CypherAstBase rightExpression, ExpressionScope scope) {
        if (left == null) {
            Object right = executeExpression(ctx, rightExpression, scope);
            if (right == null) {
                return null;
            }
            if (right instanceof Boolean) {
                boolean b = (boolean) right;
                if (b) {
                    return true;
                } else {
                    return null;
                }
            }
        }

        if (left instanceof Boolean) {
            boolean bLeft = (boolean) left;
            if (bLeft) {
                return true;
            }
            Object right = executeExpression(ctx, rightExpression, scope);
            if (right == null) {
                return null;
            }
            if (right instanceof Boolean) {
                return right;
            }

            throw new VertexiumException("unexpected value in OR expression: " + right);
        }

        throw new VertexiumException("unexpected value in OR expression: " + left);
    }

    private Object executeXOR(VertexiumCypherQueryContext ctx, Object left, CypherAstBase rightExpression, ExpressionScope scope) {
        if (left == null) {
            return null;
        }

        Object right = executeExpression(ctx, rightExpression, scope);
        if (right == null) {
            return null;
        }

        throw new VertexiumCypherNotImplemented("XOR " + left + ", " + right);
    }

    private Object executeVariable(VertexiumCypherQueryContext ctx, CypherVariable expression, ExpressionScope scope) {
        if (scope == null) {
            throw new VertexiumException("Could not get variable \"" + expression.getName() + "\" last results were null");
        }
        return scope.getByName(expression.getName());
    }

    Stream<VertexiumCypherScope.Item> applyWhereToResults(
            VertexiumCypherQueryContext ctx,
            Stream<VertexiumCypherScope.Item> rows,
            CypherAstBase whereExpression
    ) {
        return rows
                .filter(row -> {
                    Object result = executeExpression(ctx, whereExpression, row);
                    return ObjectUtils.compare(true, result) == 0;
                });
    }
}
