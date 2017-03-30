package org.vertexium.cypher.ast.model;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CypherListLiteral<TItem> extends CypherLiteral<List<TItem>> implements Iterable<TItem> {
    public CypherListLiteral(List<TItem> list) {
        super(list);
    }

    public CypherListLiteral() {
        this(new ArrayList<>());
    }

    @SuppressWarnings("unchecked")
    public TItem[] toArray(TItem[] arr) {
        return getValue().toArray(arr);
    }

    public static <T> Collector<T, ArrayList<T>, CypherListLiteral<T>> collect() {
        return Collector.of(
                ArrayList::new,
                ArrayList::add,
                (list1, list2) -> {
                    list1.addAll(list2);
                    return list1;
                },
                CypherListLiteral::new
        );
    }

    public int size() {
        return getValue().size();
    }

    public TItem get(int i) {
        return getValue().get(i);
    }

    @Override
    public Iterator<TItem> iterator() {
        return getValue().iterator();
    }

    public Stream<TItem> stream() {
        return getValue().stream();
    }

    @Override
    public String toString() {
        String delimiter;
        if (size() > 0 && get(0) instanceof CypherLabelName) {
            delimiter = "";
        } else {
            delimiter = ", ";
        }
        return toString(delimiter);
    }

    public String toString(String delimiter) {
        return stream().map(o -> o == null ? "null" : o.toString()).collect(Collectors.joining(delimiter));
    }

    public static <T> CypherListLiteral<T> of(T... items) {
        return new CypherListLiteral<>(Lists.newArrayList(items));
    }
}
