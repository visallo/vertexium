package org.vertexium.cypher.ast.model;

import java.util.stream.Stream;

public class CypherReturnBody extends CypherAstBase {
    private final CypherListLiteral<CypherReturnItem> returnItems;
    private final CypherOrderBy order;
    private final CypherLimit limit;
    private final CypherSkip skip;

    public CypherReturnBody(
        CypherListLiteral<CypherReturnItem> returnItems,
        CypherOrderBy order,
        CypherLimit limit,
        CypherSkip skip
    ) {
        this.returnItems = returnItems;
        this.order = order;
        this.limit = limit;
        this.skip = skip;
    }

    public CypherListLiteral<CypherReturnItem> getReturnItems() {
        return returnItems;
    }

    public CypherOrderBy getOrder() {
        return order;
    }

    public CypherLimit getLimit() {
        return limit;
    }

    public CypherSkip getSkip() {
        return skip;
    }

    @Override
    public String toString() {
        return String.format(
            "%s%s%s%s",
            getReturnItems(),
            getOrder() == null ? "" : " " + getOrder(),
            getLimit() == null ? "" : " " + getLimit(),
            getSkip() == null ? "" : " " + getSkip()
        );
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return Stream.concat(
            returnItems.stream(),
            Stream.of(order, limit, skip)
        );
    }
}
