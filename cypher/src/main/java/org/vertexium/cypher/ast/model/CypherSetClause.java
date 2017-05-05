package org.vertexium.cypher.ast.model;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CypherSetClause extends CypherClause {
    private final List<CypherSetItem> setItems;

    public CypherSetClause(List<CypherSetItem> setItems) {
        this.setItems = setItems;
    }

    public List<CypherSetItem> getSetItems() {
        return setItems;
    }

    @Override
    public String toString() {
        return String.format(
                "SET %s",
                getSetItems().stream().map(Object::toString).collect(Collectors.joining(", "))
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return setItems.stream();
    }
}
