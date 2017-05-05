package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherReturnClause extends CypherClause {
    private final boolean distinct;
    private final CypherReturnBody returnBody;

    public CypherReturnClause(boolean distinct, CypherReturnBody returnBody) {
        this.distinct = distinct;
        this.returnBody = returnBody;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public CypherReturnBody getReturnBody() {
        return returnBody;
    }

    @Override
    public String toString() {
        return String.format(
                "RETURN %s%s",
                isDistinct() ? "DISTINCT " : "",
                getReturnBody().toString()
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.of(returnBody);
    }
}
