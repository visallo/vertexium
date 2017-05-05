package org.vertexium.cypher.executor;

import org.vertexium.cypher.VertexiumCypherScope;

import java.util.Collection;
import java.util.LinkedHashSet;

public interface ExpressionScope {
    Object getByName(String name);

    boolean contains(String name);

    ExpressionScope getParentScope();

    VertexiumCypherScope getParentCypherScope();

    LinkedHashSet<String> getColumnNames();
}
