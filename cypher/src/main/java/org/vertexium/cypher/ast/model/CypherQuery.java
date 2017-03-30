package org.vertexium.cypher.ast.model;

import com.google.common.collect.ImmutableList;

import java.util.stream.Stream;

public class CypherQuery extends CypherAstBase {
    private final ImmutableList<CypherClause> clauses;

    public CypherQuery(ImmutableList<CypherClause> clauses) {
        this.clauses = clauses;
    }

    public ImmutableList<CypherClause> getClauses() {
        return clauses;
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return clauses.stream();
    }
}
