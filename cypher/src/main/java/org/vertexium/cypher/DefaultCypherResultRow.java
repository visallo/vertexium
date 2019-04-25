package org.vertexium.cypher;

import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.exceptions.VertexiumCypherException;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultCypherResultRow implements CypherResultRow {
    private final LinkedHashSet<String> columnNames;
    private final Map<String, Object> values;
    private final List<Map<String, Object>> scopes = new ArrayList<>();

    public DefaultCypherResultRow(LinkedHashSet<String> columnNames, Map<String, Object> values) {
        this.columnNames = columnNames;
        this.values = values;
    }

    @Override
    public Object get(String name) {
        for (int i = scopes.size() - 1; i >= 0; i--) {
            Map<String, Object> scope = scopes.get(i);
            if (scope.containsKey(name)) {
                return scope.get(name);
            }
        }
        return values.get(name);
    }

    @Override
    public boolean has(String name) {
        for (int i = scopes.size() - 1; i >= 0; i--) {
            Map<String, Object> scope = scopes.get(i);
            if (scope.containsKey(name)) {
                return true;
            }
        }
        return values.containsKey(name);
    }

    @Override
    public CypherResultRow set(String name, Object value) {
        if (value instanceof CypherAstBase) {
            throw new VertexiumCypherException("Should not put cypher ast values in row");
        }
        values.put(name, value);
        return this;
    }

    @Override
    public CypherResultRow pushScope(String name, Object value) {
        if (value instanceof CypherAstBase) {
            throw new VertexiumCypherException("Should not put cypher ast values in row");
        }
        Map<String, Object> map = new HashMap<>();
        map.put(name, value);
        pushScope(map);
        return this;
    }

    @Override
    public CypherResultRow pushScope(Map<String, Object> scope) {
        scopes.add(scope);
        return this;
    }

    @Override
    public void popScope(int count) {
        if (scopes.size() < count) {
            throw new VertexiumCypherException("Not enough scopes to pop. Expected " + count + " found " + scopes.size());
        }
        for (int i = 0; i < count; i++) {
            scopes.remove(scopes.size() - 1);
        }
    }

    @Override
    public Map<String, Object> popScope() {
        if (scopes.size() == 0) {
            throw new VertexiumCypherException("No scopes to pop");
        }
        return scopes.remove(scopes.size() - 1);
    }

    @Override
    public Stream<String> getNames() {
        Set<String> names = new HashSet<>(values.keySet());
        for (Map<String, Object> scope : scopes) {
            names.addAll(scope.keySet());
        }
        return names.stream();
    }

    @Override
    public CypherResultRow clone() {
        DefaultCypherResultRow result = new DefaultCypherResultRow(new LinkedHashSet<>(columnNames), new HashMap<>(values));
        for (Map<String, Object> scope : scopes) {
            result.scopes.add(new HashMap<>(scope));
        }
        return result;
    }

    @Override
    public LinkedHashSet<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public String toString() {
        return String.format(
            "%s {columnNames=%s, values={%s}}",
            getClass().getName(),
            String.join(", ", columnNames),
            getNames()
                .map(name -> String.format("%s=%s", name, get(name)))
                .collect(Collectors.joining(", "))
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }

        DefaultCypherResultRow that = (DefaultCypherResultRow) o;

        List<String> names = getNames().sorted().collect(Collectors.toList());
        List<String> thatNames = that.getNames().sorted().collect(Collectors.toList());
        if (names.size() != thatNames.size()) {
            return false;
        }
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            String thatName = thatNames.get(i);
            if (!name.equals(thatName)) {
                return false;
            }

            Object value = get(name);
            Object thatValue = get(name);
            if (!Objects.equals(value, thatValue)) {
                return false;
            }
        }

        return Objects.equals(getColumnNames(), that.getColumnNames());
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnNames, values, scopes);
    }
}
