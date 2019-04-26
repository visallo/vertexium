package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherWithClause extends CypherClause {
    private final boolean distinct;
    private final CypherReturnBody returnBody;
    private final CypherAstBase where;

    public CypherWithClause(boolean distinct, CypherReturnBody returnBody, CypherAstBase where) {
        this.distinct = distinct;
        this.returnBody = returnBody;
        this.where = where;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public CypherReturnBody getReturnBody() {
        return returnBody;
    }

    public CypherAstBase getWhere() {
        return where;
    }

    @Override
    public String toString() {
        return String.format(
            "WITH %s%s%s",
            isDistinct() ? "DISTINCT " : "",
            getReturnBody().toString(),
            getWhere() == null ? "" : " WHERE " + getWhere().toString()
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(returnBody, where);
    }
}
