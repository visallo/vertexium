package org.vertexium.cypher.ast.model;

import org.vertexium.VertexiumException;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

public class CypherLiteral<T> extends CypherAstBase implements Comparable {
    private final T value;

    public CypherLiteral(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    @Override
    public int compareTo(Object o) {
        if (!(getValue() instanceof Comparable)) {
            throw new VertexiumException("Literal value is not comparable (type: " + getValue().getClass().getName() + ")");
        }
        Comparable value = (Comparable) getValue();
        if (o instanceof CypherLiteral) {
            o = ((CypherLiteral) o).getValue();
        }
        return value.compareTo(o);
    }

    @Override
    public String toString() {
        if (getValue() == null) {
            return "null";
        }
        return getValue().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CypherLiteral<?> literal = (CypherLiteral<?>) o;

        if (value != null ? !value.equals(literal.value) : literal.value != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public Stream<? extends CypherAstBase> getChildren() {
        if (value instanceof CypherAstBase) {
            return Stream.of((CypherAstBase) value);
        }
        return Stream.of();
    }

    public static Object toJava(Object value) {
        if (value instanceof Iterable) {
            value = stream((Iterable<Object>) value)
                .map(CypherLiteral::toJava)
                .collect(Collectors.toList());
        }

        if (value instanceof CypherLiteral) {
            value = ((CypherLiteral) value).getValue();
        }

        return value;
    }
}
