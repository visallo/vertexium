package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherFilterExpression extends CypherAstBase {
    private final CypherIdInColl idInCol;
    private final CypherAstBase where;

    public CypherFilterExpression(CypherIdInColl idInCol, CypherAstBase where) {
        this.idInCol = idInCol;
        this.where = where;
    }

    public CypherIdInColl getIdInCol() {
        return idInCol;
    }

    public CypherAstBase getWhere() {
        return where;
    }

    @Override
    public String toString() {
        return String.format(
            "%s%s",
            getIdInCol(),
            getWhere() == null ? "" : " " + getWhere()
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(idInCol, where);
    }
}
