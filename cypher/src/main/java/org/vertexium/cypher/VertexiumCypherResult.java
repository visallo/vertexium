package org.vertexium.cypher;

import java.util.LinkedHashSet;
import java.util.stream.Stream;

public interface VertexiumCypherResult {
    int size();

    LinkedHashSet<String> getColumnNames();

    Stream<? extends Row> stream();

    interface Row {
        Object getByName(String columnName);
    }
}
