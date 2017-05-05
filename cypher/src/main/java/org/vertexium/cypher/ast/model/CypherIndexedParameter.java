package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherIndexedParameter extends CypherParameter {
    private final int index;

    public CypherIndexedParameter(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return String.format("$%d", getIndex());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.empty();
    }
}
