package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherUnion extends CypherAstBase {
    private final CypherQuery left;
    private final CypherAstBase right;
    private final boolean all;

    public CypherUnion(CypherQuery left, CypherAstBase right, boolean all) {
        this.left = left;
        this.right = right;
        this.all = all;
    }

    public CypherQuery getLeft() {
        return left;
    }

    public CypherAstBase getRight() {
        return right;
    }

    public boolean isAll() {
        return all;
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(left, right);
    }
}
