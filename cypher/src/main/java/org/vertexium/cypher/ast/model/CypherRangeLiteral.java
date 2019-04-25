package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherRangeLiteral extends CypherAstBase {
    private final Integer from;
    private final Integer to;

    public CypherRangeLiteral(Integer from, Integer to) {
        this.from = from;
        this.to = to;
    }

    public Integer getFrom() {
        return from;
    }

    public Integer getTo() {
        return to;
    }

    @Override
    public String toString() {
        if (getFrom() == null && getTo() == null) {
            return "";
        }
        return String.format("%s..%s", getFrom() == null ? "" : getFrom(), getTo() == null ? "" : getTo());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.empty();
    }

    public boolean isInRange(int length) {
        boolean fromMatch = getFrom() == null || length >= getFrom();
        boolean toMatch = getTo() == null || length <= getTo();
        return fromMatch && toMatch;
    }
}
