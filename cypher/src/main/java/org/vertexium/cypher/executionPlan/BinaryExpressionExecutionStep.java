package org.vertexium.cypher.executionPlan;

import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.ast.model.CypherBinaryExpression;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.utils.ObjectUtils;

public class BinaryExpressionExecutionStep extends ExecutionStepWithChildren implements ExecutionStepWithResultName {
    private final String resultName;
    private final CypherBinaryExpression.Op op;
    private final String leftResultName;
    private final String rightResultName;

    public BinaryExpressionExecutionStep(
        String resultName,
        ExecutionStepWithResultName left,
        ExecutionStepWithResultName right,
        CypherBinaryExpression.Op op
    ) {
        super(left, right);
        this.resultName = resultName;
        this.leftResultName = left.getResultName();
        this.rightResultName = right.getResultName();
        this.op = op;
    }

    @Override
    public VertexiumCypherResult execute(VertexiumCypherQueryContext ctx, VertexiumCypherResult source) {
        source = super.execute(ctx, source);
        return source.peek(row -> {
            Object leftValue = row.get(leftResultName);
            Object rightValue = row.get(rightResultName);
            row.popScope(2);
            Object result;
            switch (op) {
                case AND:
                    result = andValues(leftValue, rightValue);
                    break;
                case OR:
                    result = orValues(leftValue, rightValue);
                    break;
                case XOR:
                    result = xorValues(leftValue, rightValue);
                    break;
                case ADD:
                    result = addValues(leftValue, rightValue);
                    break;
                case MINUS:
                    result = minusValues(leftValue, rightValue);
                    break;
                case MULTIPLY:
                    result = multiplyValues(leftValue, rightValue);
                    break;
                case DIVIDE:
                    result = divideValues(leftValue, rightValue);
                    break;
                case MOD:
                    result = modValues(leftValue, rightValue);
                    break;
                case POWER:
                    result = powerValues(leftValue, rightValue);
                    break;
                default:
                    throw new VertexiumCypherNotImplemented("Unhandled op: " + op);
            }
            row.pushScope(resultName, result);
        });
    }

    private Object addValues(Object leftValue, Object rightValue) {
        if (leftValue == null || rightValue == null) {
            return null;
        }
        if (leftValue instanceof String) {
            return ((String) leftValue) + rightValue;
        }
        if (leftValue instanceof Number && rightValue instanceof Number) {
            Number leftNumber = (Number) leftValue;
            Number rightNumber = (Number) rightValue;
            return ObjectUtils.addNumbers(leftNumber, rightNumber);
        }
        if (leftValue instanceof Object[] && rightValue instanceof Object[]) {
            Object[] leftArr = (Object[]) leftValue;
            Object[] rightArr = (Object[]) rightValue;
            Object[] result = new Object[leftArr.length + rightArr.length];
            System.arraycopy(leftArr, 0, result, 0, leftArr.length);
            System.arraycopy(rightArr, 0, result, leftArr.length, rightArr.length);
            return result;
        }
        throw new VertexiumCypherNotImplemented("Cannot add " + leftValue + " + " + rightValue);
    }

    private Object minusValues(Object leftValue, Object rightValue) {
        Object result;
        if (leftValue == null || rightValue == null) {
            return null;
        }
        if (!(leftValue instanceof Number) || !(rightValue instanceof Number)) {
            throw new VertexiumCypherNotImplemented("cannot subtract non-numbers");
        }
        result = ObjectUtils.subtractNumbers((Number) leftValue, (Number) rightValue);
        return result;
    }

    private Object multiplyValues(Object leftValue, Object rightValue) {
        Object result;
        if (leftValue == null || rightValue == null) {
            return null;
        }
        if (!(leftValue instanceof Number) || !(rightValue instanceof Number)) {
            throw new VertexiumCypherNotImplemented("cannot multiply non-numbers");
        }
        result = ObjectUtils.multiplyNumbers((Number) leftValue, (Number) rightValue);
        return result;
    }

    private Object divideValues(Object leftValue, Object rightValue) {
        Object result;
        if (leftValue == null || rightValue == null) {
            return null;
        }
        if (!(leftValue instanceof Number) || !(rightValue instanceof Number)) {
            throw new VertexiumCypherNotImplemented("cannot divide non-numbers");
        }
        result = ObjectUtils.divideNumbers((Number) leftValue, (Number) rightValue);
        return result;
    }

    private Object modValues(Object leftValue, Object rightValue) {
        Object result;
        if (leftValue == null || rightValue == null) {
            return null;
        }
        if (!(leftValue instanceof Number) || !(rightValue instanceof Number)) {
            throw new VertexiumCypherNotImplemented("cannot mod non-numbers");
        }
        result = ObjectUtils.modNumbers((Number) leftValue, (Number) rightValue);
        return result;
    }

    private Object powerValues(Object leftValue, Object rightValue) {
        Object result;
        if (leftValue == null || rightValue == null) {
            return null;
        }
        if (!(leftValue instanceof Number) || !(rightValue instanceof Number)) {
            throw new VertexiumCypherNotImplemented("cannot power non-numbers");
        }
        result = ObjectUtils.powerNumbers((Number) leftValue, (Number) rightValue);
        return result;
    }

    private Boolean andValues(Object leftValue, Object rightValue) {
        if (leftValue == null || rightValue == null) {
            if (((leftValue != null) && (!((boolean) leftValue)))) {
                return false;
            }
            if (((rightValue != null) && (!((boolean) rightValue)))) {
                return false;
            }
            return null;
        }
        return ((boolean) leftValue) && ((boolean) rightValue);
    }

    private Boolean orValues(Object leftValue, Object rightValue) {
        if (leftValue == null || rightValue == null) {
            if (((leftValue != null) && ((boolean) leftValue))) {
                return true;
            }
            if (((rightValue != null) && ((boolean) rightValue))) {
                return true;
            }
            return null;
        }
        return ((boolean) leftValue) || ((boolean) rightValue);
    }

    private Boolean xorValues(Object leftValue, Object rightValue) {
        if (leftValue == null || rightValue == null) {
            return null;
        }
        return ((boolean) leftValue) ^ ((boolean) rightValue);
    }

    @Override
    public String toString() {
        return String.format(
            "%s {resultName='%s', op=%s, leftResultName='%s', rightResultName='%s'}",
            super.toString(),
            resultName,
            op,
            leftResultName,
            rightResultName
        );
    }

    @Override
    public String getResultName() {
        return resultName;
    }
}
