package org.vertexium.sql;

import org.vertexium.inmemory.InMemoryTableElement;
import org.vertexium.inmemory.InMemoryVertex;
import org.vertexium.inmemory.InMemoryVertexTable;
import org.vertexium.sql.collections.SqlMap;

public class SqlVertexTable extends InMemoryVertexTable {

    public SqlVertexTable(SqlMap<InMemoryTableElement<InMemoryVertex>> rows) {
        super(rows);
    }

    @Override
    protected InMemoryTableElement<InMemoryVertex> createInMemoryTableElement(String id) {
        return new SqlTableVertex(id);
    }
}
