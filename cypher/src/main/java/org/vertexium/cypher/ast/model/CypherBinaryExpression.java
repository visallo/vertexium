package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherBinaryExpression extends CypherExpression {
    private final CypherAstBase left;
    private final Op op;
    private final CypherAstBase right;

    public CypherBinaryExpression(CypherAstBase left, Op op, CypherAstBase right) {
        this.left = left;
        this.op = op;
        this.right = right;
    }

    public CypherAstBase getLeft() {
        return left;
    }

    public Op getOp() {
        return op;
    }

    public CypherAstBase getRight() {
        return right;
    }

    @Override
    public String toString() {
        return String.format(
                "%s %s %s",
                getLeft(),
                getOp().text,
                getRight()
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(left, right);
    }

    public enum Op {
        ADD("+"),
        MINUS("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        POWER("^"),
        MOD("%"),
        OR("OR"),
        XOR("XOR"),
        AND("AND");

        private final String text;

        Op(String text) {
            this.text = text;
        }

        public static Op parseOrNull(String text) {
            if (text.trim().length() == 0) {
                return null;
            }
            for (Op op : Op.values()) {
                if (op.text.equals(text)) {
                    return op;
                }
            }
            return null;
        }
    }
}
