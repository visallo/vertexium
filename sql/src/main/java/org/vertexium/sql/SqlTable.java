package org.vertexium.sql;

import org.vertexium.inmemory.InMemoryElement;
import org.vertexium.inmemory.InMemoryTable;
import org.vertexium.inmemory.InMemoryTableElement;

import java.util.Map;

abstract class SqlTable<TElement extends InMemoryElement> extends InMemoryTable<TElement> {
    protected SqlTable(Map<String, InMemoryTableElement<TElement>> rows) {
        super(rows);
    }
}
