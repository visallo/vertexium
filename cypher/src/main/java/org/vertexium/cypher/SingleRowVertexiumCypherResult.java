package org.vertexium.cypher;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.stream.Stream;

public class SingleRowVertexiumCypherResult extends VertexiumCypherResult {
    public SingleRowVertexiumCypherResult() {
        this(new DefaultCypherResultRow(new LinkedHashSet<>(), new HashMap<>()));
    }

    public SingleRowVertexiumCypherResult(CypherResultRow row) {
        super(Stream.of(row), row.getColumnNames());
    }
}
