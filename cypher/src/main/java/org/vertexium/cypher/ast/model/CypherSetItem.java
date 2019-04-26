package org.vertexium.cypher.ast.model;

import org.vertexium.VertexiumException;

import java.util.stream.Stream;

public abstract class CypherSetItem<TLeft extends CypherAstBase, TRight extends CypherAstBase> extends CypherAstBase {
    private final TLeft left;
    private final Op op;
    private final TRight right;

    public CypherSetItem(TLeft left, Op op, TRight right) {
        this.left = left;
        this.op = op;
        this.right = right;
    }

    public TLeft getLeft() {
        return left;
    }

    public Op getOp() {
        return op;
    }

    public TRight getRight() {
        return right;
    }

    @Override
    public String toString() {
        // should be a list of label names
        if (getRight() instanceof CypherListLiteral) {
            return String.format("%s%s", getLeft(), getRight());
        }
        return String.format(
            "%s %s %s",
            getLeft(),
            getOp(),
            getRight()
        );
    }

    public enum Op {
        PLUS_EQUAL,
        EQUAL;

        public String toString() {
            switch (this) {
                case PLUS_EQUAL:
                    return "+=";
                case EQUAL:
                    return "=";
                default:
                    throw new VertexiumException("unhandled op: " + this);
            }
        }
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(getLeft(), getRight());
    }
}
