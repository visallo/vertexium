package org.vertexium.cypher.ast.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class CypherMapLiteral<TKey, TValue> extends CypherLiteral<Map<TKey, TValue>> {
    public CypherMapLiteral(Map<TKey, TValue> map) {
        super(map);
    }

    public CypherMapLiteral() {
        this(new HashMap<>());
    }

    public int size() {
        return getValue().size();
    }

    public Iterable<? extends Map.Entry<TKey, TValue>> entrySet() {
        return getValue().entrySet();
    }

    public TValue get(TKey key) {
        return getValue().get(key);
    }

    public Map<TKey, TValue> toMap() {
        return getValue();
    }

    public Set<TKey> getKeys() {
        return getValue().keySet();
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        return getValue().entrySet().stream()
            .flatMap(entry -> {
                if (entry.getKey() instanceof CypherAstBase && entry.getValue() instanceof CypherAstBase) {
                    return Stream.of((CypherAstBase) entry.getKey(), (CypherAstBase) entry.getValue());
                }
                if (entry.getValue() instanceof CypherAstBase) {
                    return Stream.of((CypherAstBase) entry.getValue());
                }
                if (entry.getKey() instanceof CypherAstBase) {
                    return Stream.of((CypherAstBase) entry.getKey());
                }
                return Stream.empty();
            });
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("{");
        boolean first = true;
        for (Map.Entry<TKey, TValue> entry : getValue().entrySet()) {
            if (!first) {
                result.append(", ");
            }
            result.append(entry.getKey());
            result.append(": ");
            result.append(entry.getValue());
            first = false;
        }
        result.append("}");
        return result.toString();
    }
}
