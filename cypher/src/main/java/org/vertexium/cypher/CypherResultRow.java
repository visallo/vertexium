package org.vertexium.cypher;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.stream.Stream;

public interface CypherResultRow {
    Object get(String name);

    boolean has(String name);

    LinkedHashSet<String> getColumnNames();

    CypherResultRow set(String name, Object value);

    CypherResultRow pushScope(String name, Object value);

    CypherResultRow pushScope(Map<String, Object> scope);

    void popScope(int count);

    Map<String, Object> popScope();

    Stream<String> getNames();

    CypherResultRow clone();

    default Object[] get(String[] names) {
        Object[] results = new Object[names.length];
        for (int i = 0; i < names.length; i++) {
            results[i] = get(names[i]);
        }
        return results;
    }
}
