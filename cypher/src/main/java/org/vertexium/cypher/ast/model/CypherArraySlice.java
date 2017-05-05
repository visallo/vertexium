package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherArraySlice extends CypherAstBase {
    private final CypherAstBase arrayExpression;
    private final CypherAstBase sliceFrom;
    private final CypherAstBase sliceTo;

    public CypherArraySlice(CypherAstBase arrayExpression, CypherAstBase sliceFrom, CypherAstBase sliceTo) {
        this.arrayExpression = arrayExpression;
        this.sliceFrom = sliceFrom;
        this.sliceTo = sliceTo;
    }

    public CypherAstBase getArrayExpression() {
        return arrayExpression;
    }

    public CypherAstBase getSliceFrom() {
        return sliceFrom;
    }

    public CypherAstBase getSliceTo() {
        return sliceTo;
    }

    @Override
    public String toString() {
        return String.format("%s[%s..%s]", getArrayExpression(), getSliceFrom(), getSliceTo());
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(arrayExpression, sliceFrom, sliceTo);
    }
}
