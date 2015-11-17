package org.vertexium.sql;

import org.vertexium.inmemory.*;
import org.vertexium.sql.collections.SqlMap;
import org.vertexium.util.ConvertingIterable;

public class SqlEdgeTable extends InMemoryEdgeTable {

    public SqlEdgeTable(SqlMap<InMemoryTableElement<InMemoryEdge>> rows) {
        super(rows);
    }

    @Override
    protected InMemoryTableElement<InMemoryEdge> createInMemoryTableElement(String id) {
        return new SqlTableEdge(id);
    }

    @Override
    public Iterable<InMemoryTableEdge> getAllTableElements() {
        return new ConvertingIterable<InMemoryTableElement<InMemoryEdge>, InMemoryTableEdge>(super.getRowValues()) {
            @Override
            protected InMemoryTableEdge convert(InMemoryTableElement<InMemoryEdge> inMemoryTableElement) {
                return ((SqlTableEdge) inMemoryTableElement).asInMemoryTableElement();
            }
        };
    }
}
