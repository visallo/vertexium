package org.vertexium.query;

import org.vertexium.*;

import java.util.Iterator;

public class ExtendedDataQueryableIterable implements QueryableIterable<ExtendedDataRow> {
    private final Graph graph;
    private final Element element;
    private final String tableName;
    private final Iterable<? extends ExtendedDataRow> rows;

    public ExtendedDataQueryableIterable(Graph graph, Element element, String tableName, Iterable<? extends ExtendedDataRow> rows) {
        this.graph = graph;
        this.element = element;
        this.tableName = tableName;
        this.rows = rows;
    }

    public Graph getGraph() {
        return graph;
    }

    public Element getElement() {
        return element;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public Query query(Authorizations authorizations) {
        return query(null, authorizations);
    }

    @Override
    public Query query(String queryString, Authorizations authorizations) {
        return getGraph().getSearchIndex().queryExtendedData(
            getGraph(),
            getElement(),
            getTableName(),
            queryString,
            authorizations
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<ExtendedDataRow> iterator() {
        return (Iterator<ExtendedDataRow>) rows.iterator();
    }
}
