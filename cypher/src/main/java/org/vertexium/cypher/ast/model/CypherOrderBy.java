package org.vertexium.cypher.ast.model;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CypherOrderBy extends CypherAstBase {
    private final List<CypherSortItem> sortItems;

    public CypherOrderBy(List<CypherSortItem> sortItems) {
        this.sortItems = sortItems;
    }

    public List<CypherSortItem> getSortItems() {
        return sortItems;
    }

    @Override
    public String toString() {
        return String.format(
                "ORDER BY %s",
                getSortItems().stream().map(CypherSortItem::toString).collect(Collectors.joining(", "))
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return sortItems.stream();
    }
}
