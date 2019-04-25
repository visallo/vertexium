package org.vertexium.cypher;

import java.util.LinkedHashSet;
import java.util.stream.Stream;

public class EmptyVertexiumCypherResult extends VertexiumCypherResult {
    public EmptyVertexiumCypherResult() {
        super(Stream.empty(), new LinkedHashSet<>());
    }

    public EmptyVertexiumCypherResult(LinkedHashSet<String> columnNames) {
        super(Stream.empty(), columnNames);
    }
}
