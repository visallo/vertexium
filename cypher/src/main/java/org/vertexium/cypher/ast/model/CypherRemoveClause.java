package org.vertexium.cypher.ast.model;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CypherRemoveClause extends CypherClause {
    private final List<CypherRemoveItem> removeItems;

    public CypherRemoveClause(List<CypherRemoveItem> removeItems) {
        this.removeItems = removeItems;
    }

    public List<CypherRemoveItem> getRemoveItems() {
        return removeItems;
    }

    @Override
    public String toString() {
        return String.format(
            "REMOVE %s",
            getRemoveItems().stream().map(Object::toString).collect(Collectors.joining(", "))
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return removeItems.stream();
    }
}
