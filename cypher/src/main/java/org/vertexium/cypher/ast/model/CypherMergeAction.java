package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public abstract class CypherMergeAction extends CypherAstBase {
    private final CypherSetClause set;

    public CypherMergeAction(CypherSetClause set) {
        this.set = set;
    }

    public CypherSetClause getSet() {
        return set;
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(getSet());
    }
}
