package org.vertexium.query;

import org.vertexium.*;

public class DefaultExtendedDataQuery extends QueryBase {
    private final Element element;
    private final String tableName;

    public DefaultExtendedDataQuery(
            Graph graph,
            Element element,
            String tableName,
            String queryString,
            Authorizations authorizations
    ) {
        super(graph, queryString, authorizations);
        this.element = element;
        this.tableName = tableName;
    }

    public Element getElement() {
        return element;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean isAggregationSupported(Aggregation aggregation) {
        if (DefaultGraphQueryIterableWithAggregations.isAggregationSupported(aggregation)) {
            return true;
        }
        return super.isAggregationSupported(aggregation);
    }

    @Override
    protected QueryResultsIterable<? extends VertexiumObject> extendedData(FetchHints fetchHints) {
        return new DefaultGraphQueryIterableWithAggregations<>(
                getParameters(),
                getElement().getExtendedData(getTableName()),
                true,
                true,
                true,
                getAggregations()
        );
    }

    @Override
    public String toString() {
        return super.toString() +
                ", element=" + element +
                ", tableName=" + tableName;
    }
}
