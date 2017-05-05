package org.vertexium.cypher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.cypher.exceptions.VertexiumCypherNotImplemented;
import org.vertexium.cypher.executor.ExpressionScope;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

public class VertexiumCypherScope implements VertexiumCypherResult, ExpressionScope {
    private final Collection<Item> items;
    private final LinkedHashSet<String> columnNames;
    private final VertexiumCypherScope parentScope;

    private VertexiumCypherScope(Collection<Item> items, LinkedHashSet<String> columnNames, VertexiumCypherScope parentScope) {
        this.items = items;
        for (Item item : items) {
            if (item.parentScope == null || parentScope == null) {
                item.parentScope = this;
            }
        }
        this.columnNames = columnNames;
        this.parentScope = parentScope;
    }

    @Override
    public int size() {
        return items.size();
    }

    public VertexiumCypherScope getParentScope() {
        return parentScope;
    }

    @Override
    public VertexiumCypherScope getParentCypherScope() {
        return parentScope;
    }

    @Override
    public LinkedHashSet<String> getColumnNames() {
        return columnNames;
    }

    public Stream<Item> stream() {
        return items.stream();
    }

    public static VertexiumCypherScope newSingleItemScope(Item item) {
        return newItemsScope(Lists.newArrayList(item), null);
    }

    public static VertexiumCypherScope newItemsScope(List<Item> items, VertexiumCypherScope parentScope) {
        return new VertexiumCypherScope(items, getColumnNames(items), parentScope);
    }

    public List<Object> getByName(String name) {
        List<Object> results = items.stream()
                .map(i -> i.getByName(name, false))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (results.size() == 0 && getParentScope() != null) {
            return getParentScope().getByName(name);
        }
        return results;
    }

    @Override
    public boolean contains(String name) {
        boolean results = items.stream()
                .anyMatch(i -> i.contains(name, false));
        if (results) {
            return true;
        }
        if (getParentScope() != null && getParentScope().contains(name)) {
            return true;
        }
        return false;
    }

    public VertexiumCypherScope concat(VertexiumCypherScope other, VertexiumCypherScope parentScope) {
        return concat(other, false, parentScope);
    }

    public VertexiumCypherScope concat(VertexiumCypherScope other, boolean distinct, VertexiumCypherScope parentScope) {
        Collection<Item> newItems = distinct ? new LinkedHashSet<>() : new ArrayList<>();
        newItems.addAll(items);
        newItems.addAll(other.items);

        LinkedHashSet<String> allColumnNames = new LinkedHashSet<>(getColumnNames());
        allColumnNames.addAll(other.getColumnNames());

        return new VertexiumCypherScope(newItems, allColumnNames, parentScope);
    }

    public static Collector<VertexiumCypherScope, Builder, VertexiumCypherScope> concatStreams(VertexiumCypherScope parentScope) {
        return Collector.of(
                () -> new Builder(parentScope),
                Builder::add,
                Builder::concat,
                Builder::build
        );
    }

    public static VertexiumCypherScope newEmpty() {
        return new VertexiumCypherScope(new ArrayList<>(), new LinkedHashSet<>(), null);
    }

    public VertexiumCypherScope cartesianProduct(VertexiumCypherScope other, VertexiumCypherScope parentScope) {
        List<Item> allItems = new ArrayList<>();
        for (Item item : items) {
            for (Item itemOther : other.items) {
                allItems.add(item.concat(itemOther));
            }
        }
        return newFromItems(allItems.stream(), parentScope);
    }

    public static VertexiumCypherScope newFromItems(Stream<Item> items, VertexiumCypherScope parentScope) {
        List<Item> itemsList = items.collect(Collectors.toList());
        return newFromItems(itemsList.stream(), getColumnNames(itemsList), parentScope);
    }

    private static LinkedHashSet<String> getColumnNames(List<Item> itemsList) {
        LinkedHashSet<String> results = new LinkedHashSet<>();
        for (Item item : itemsList) {
            results.addAll(item.getColumnNames());
        }
        return results;
    }

    public static VertexiumCypherScope newFromItems(Stream<Item> items, LinkedHashSet<String> columnNames, VertexiumCypherScope parentScope) {
        return new VertexiumCypherScope(items.collect(Collectors.toList()), columnNames, parentScope);
    }

    public static Item newMapItem(String name, Object value, ExpressionScope parentScope) {
        checkNotNull(name, "name cannot be null");
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put(name, value);
        return newMapItem(map, parentScope);
    }

    public static Item newEmptyItem() {
        return new EmptyItem(null);
    }

    public static Item newMapItem(LinkedHashMap<String, ?> map, ExpressionScope parentScope) {
        return new MapItem(map, parentScope);
    }

    public static PathItem newEmptyPathItem(String pathName, ExpressionScope parentScope) {
        return new PathItem(pathName, new ArrayList<>(), parentScope);
    }

    public static abstract class Item implements VertexiumCypherResult.Row, ExpressionScope {
        private ExpressionScope parentScope;

        protected Item(ExpressionScope parentScope) {
            this.parentScope = parentScope;
        }

        public abstract LinkedHashSet<String> getColumnNames();

        public Object getByName(String name) {
            return getByName(name, true);
        }

        @Override
        public boolean contains(String name) {
            return contains(name, true);
        }

        public abstract Object getByName(String name, boolean lookInParent);

        public abstract boolean contains(String name, boolean lookInParent);

        @Override
        public ExpressionScope getParentScope() {
            return parentScope;
        }

        public abstract Item concat(Item itemOther);

        public VertexiumCypherScope getParentCypherScope() {
            ExpressionScope parentScope = getParentScope();
            while (parentScope != null) {
                if (parentScope instanceof VertexiumCypherScope) {
                    return (VertexiumCypherScope) parentScope;
                }
                parentScope = parentScope.getParentScope();
            }
            return null;
        }

        @Override
        public abstract boolean equals(Object o);

        @Override
        public abstract int hashCode();

        public static List<Item> cartesianProduct(List<Item> item0, List<Item> item1) {
            List<Item> allItems = new ArrayList<>();
            for (Item item : item0) {
                for (Item itemOther : item1) {
                    allItems.add(item.concat(itemOther));
                }
            }
            return allItems;
        }
    }

    public static class EmptyItem extends Item {
        public EmptyItem(VertexiumCypherScope parentScope) {
            super(parentScope);
        }

        @Override
        public LinkedHashSet<String> getColumnNames() {
            return new LinkedHashSet<>();
        }

        @Override
        public Object getByName(String name, boolean lookInParent) {
            if (lookInParent && getParentScope() != null) {
                return getParentScope().getByName(name);
            }
            return null;
        }

        @Override
        public boolean contains(String name, boolean lookInParent) {
            if (lookInParent && getParentScope() != null) {
                return getParentScope().contains(name);
            }
            return false;
        }

        @Override
        public Item concat(Item itemOther) {
            return itemOther;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    public static class PathItem extends VertexiumCypherScope.Item implements VertexiumCypherPath {
        private final String pathName;
        private final List<Entry> items;
        private PrintMode printMode;

        private PathItem(String pathName, List<Entry> entries, ExpressionScope parentScope) {
            super(parentScope);
            this.pathName = pathName;
            this.items = entries;
        }

        public PathItem setPrintMode(PrintMode printMode) {
            this.printMode = printMode;
            return this;
        }

        public PrintMode getPrintMode() {
            return printMode;
        }

        public Element getLastElement() {
            if (items.size() > 0) {
                return items.get(items.size() - 1).element;
            }
            throw new IndexOutOfBoundsException("list contains no items");
        }

        public Element getFirstElement() {
            if (items.size() > 0) {
                return items.get(0).element;
            }
            throw new IndexOutOfBoundsException("list contains no items");
        }

        public PathItem concat(String name, Element element) {
            List<Entry> allEntries = new ArrayList<>();
            allEntries.addAll(this.items);
            allEntries.add(new Entry(name, element));
            return new PathItem(pathName, allEntries, getParentScope())
                    .setPrintMode(getPrintMode());
        }

        public PathItem concat(PathItem pathItem) {
            List<Entry> entries = new ArrayList<>();
            entries.addAll(this.items);
            int offset = 0;
            if (entries.size() > 0 && pathItem.items.size() > 0
                    && entries.get(entries.size() - 1).element.equals(pathItem.items.get(0).element)) {
                offset = 1;
            }
            entries.addAll(pathItem.items.subList(offset, pathItem.items.size()));
            return new PathItem(pathName, entries, getParentScope());
        }

        public boolean contains(Element element) {
            return items.stream().anyMatch(e -> e.element != null && e.element.equals(element));
        }

        public List<Edge> getEdges() {
            return items.stream()
                    .filter(e -> e.element instanceof Edge)
                    .map(e -> (Edge) e.element)
                    .collect(Collectors.toList());
        }

        public List<Vertex> getVertices() {
            return items.stream()
                    .filter(e -> e.element instanceof Vertex)
                    .map(e -> (Vertex) e.element)
                    .collect(Collectors.toList());
        }

        public Element getElement(int index) {
            int calculatedIndex = index;
            if (calculatedIndex < 0) {
                calculatedIndex = items.size() + index;
            }
            if (calculatedIndex < 0 || calculatedIndex >= items.size()) {
                throw new IndexOutOfBoundsException("requested " + index + " but size is " + items.size());
            }
            return items.get(calculatedIndex).element;
        }

        @Override
        public LinkedHashSet<String> getColumnNames() {
            throw new VertexiumCypherNotImplemented("getColumnNames");
        }

        @Override
        public Object getByName(String name, boolean lookInParent) {
            throw new VertexiumCypherNotImplemented("getByName");
        }

        @Override
        public boolean contains(String name, boolean lookInParent) {
            throw new VertexiumCypherNotImplemented("contains");
        }

        @Override
        public VertexiumCypherScope.Item concat(VertexiumCypherScope.Item itemOther) {
            throw new VertexiumCypherNotImplemented("concat path items");
        }

        public String toString(VertexiumCypherQueryContext ctx) {
            if (printMode == PrintMode.RELATIONSHIP_RANGE) {
                return "[" + items.stream()
                        .filter(item -> item.element instanceof Edge)
                        .map(item -> item.toString(ctx))
                        .collect(Collectors.joining(", ")) + "]";
            }

            StringBuilder result = new StringBuilder();
            result.append("<");
            Vertex previousVertex = null;
            Element previousElement = null;
            for (Entry item : items) {
                Element element = item.element;
                if (element == null) {
                    // do nothing
                } else if (element instanceof Edge) {
                    Edge edge = (Edge) element;
                    Direction direction = null;
                    if (previousVertex != null) {
                        direction = getDirection(previousVertex.getId(), edge);
                    }
                    if (direction != null && direction == Direction.IN) {
                        result.append("<");
                    }
                    result.append("-");
                    result.append(item.toString(ctx));
                    result.append("-");
                    if (direction != null && direction == Direction.OUT) {
                        result.append(">");
                    }
                } else if (element instanceof Vertex) {
                    Vertex vertex = (Vertex) element;
                    if (previousElement == null && vertex.equals(previousVertex)) {
                        // this is a result of a zero length path
                    } else {
                        result.append(item.toString(ctx));
                        previousVertex = vertex;
                    }
                } else {
                    throw new VertexiumException("unexpected element type: " + element.getClass().getName());
                }
                previousElement = element;
            }
            result.append(">");
            return result.toString();
        }

        public String toString() {
            return toString(null);
        }

        private Direction getDirection(String previousVertexId, Edge element) {
            if (element.getVertexId(Direction.OUT).equals(previousVertexId)) {
                return Direction.OUT;
            } else {
                return Direction.IN;
            }
        }

        public int getLength() {
            return (int) items.stream()
                    .filter(e -> e.element instanceof Edge)
                    .map(e -> (Edge) e.element)
                    .count();
        }

        @Override
        public String getPathName() {
            return pathName;
        }

        @Override
        public List<VertexiumCypherPath.Item> getItems() {
            ImmutableList.Builder<VertexiumCypherPath.Item> builder = ImmutableList.builder();
            return builder
                    .addAll(items)
                    .build();
        }

        public boolean canVertexConnectOrFoundAtStartOrEnd(Vertex vertex) {
            return canVertexConnectOrFound(getFirstElement(), vertex)
                    || canVertexConnectOrFound(getLastElement(), vertex);
        }

        private boolean canVertexConnectOrFound(Element element, Vertex vertex) {
            if (element instanceof Vertex) {
                return element.equals(vertex);
            } else if (element instanceof Edge) {
                Edge edge = (Edge) element;
                return edge.getVertexId(Direction.OUT).equals(vertex.getId())
                        || edge.getVertexId(Direction.IN).equals(vertex.getId());
            }
            return false;
        }

        public enum PrintMode {RELATIONSHIP_RANGE}

        private static class Entry implements VertexiumCypherPath.Item {
            public final String name;
            public final Element element;

            private Entry(String name, Element element) {
                this.name = name;
                this.element = element;
            }

            @Override
            public String toString() {
                return "{" +
                        "name='" + name + '\'' +
                        ", " + (element instanceof Vertex ? "vertex" : "edge") + "Id=" + (element == null ? null : element.getId()) +
                        '}';
            }

            public String toString(VertexiumCypherQueryContext ctx) {
                if (ctx == null) {
                    return element.toString();
                }
                return ctx.getResultWriter().columnValueToString(ctx, element);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                Entry entry = (Entry) o;

                if (name != null ? !name.equals(entry.name) : entry.name != null) {
                    return false;
                }
                return element != null ? element.equals(entry.element) : entry.element == null;
            }

            @Override
            public int hashCode() {
                int result = name != null ? name.hashCode() : 0;
                result = 31 * result + (element != null ? element.hashCode() : 0);
                return result;
            }

            @Override
            public String getItemName() {
                return name;
            }

            @Override
            public Element getElement() {
                return element;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PathItem pathItem = (PathItem) o;

            if (pathName != null ? !pathName.equals(pathItem.pathName) : pathItem.pathName != null) {
                return false;
            }
            return items != null ? items.equals(pathItem.items) : pathItem.items == null;
        }

        @Override
        public int hashCode() {
            int result = pathName != null ? pathName.hashCode() : 0;
            result = 31 * result + (items != null ? items.hashCode() : 0);
            return result;
        }
    }

    public static class MapItem extends Item {
        private final LinkedHashMap<String, ?> map;

        public MapItem(LinkedHashMap<String, ?> map, ExpressionScope parentScope) {
            super(parentScope);
            this.map = map;
        }

        @Override
        public LinkedHashSet<String> getColumnNames() {
            LinkedHashSet<String> results = new LinkedHashSet<>();
            for (Map.Entry<String, ?> entry : map.entrySet()) {
                results.add(entry.getKey());
            }
            results.addAll(getParentScope().getColumnNames());
            return results;
        }

        @Override
        public Object getByName(String name, boolean lookInParent) {
            if (map.containsKey(name)) {
                return map.get(name);
            }
            if (lookInParent && getParentScope() != null) {
                return getParentScope().getByName(name);
            }
            return null;
        }

        @Override
        public boolean contains(String name, boolean lookInParent) {
            if (map.containsKey(name)) {
                return true;
            }
            if (lookInParent && getParentScope() != null) {
                return getParentScope().contains(name);
            }
            return false;
        }

        @Override
        public Item concat(Item itemOther) {
            if (itemOther instanceof MapItem) {
                LinkedHashMap<String, Object> all = new LinkedHashMap<>();
                all.putAll(map);
                all.putAll(((MapItem) itemOther).map);
                return new MapItem(all, getParentScope());
            }
            throw new VertexiumCypherNotImplemented("concat map items with: " + itemOther);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MapItem mapItem = (MapItem) o;

            return map.equals(mapItem.map);
        }

        @Override
        public int hashCode() {
            return map.hashCode();
        }
    }

    public static class Builder {
        private final VertexiumCypherScope parentScope;
        private final List<Item> items = new ArrayList<>();

        public Builder(VertexiumCypherScope parentScope) {
            this.parentScope = parentScope;
        }

        public void add(VertexiumCypherScope other) {
            items.addAll(other.items);
        }

        public Builder concat(Builder other) {
            throw new VertexiumCypherNotImplemented("concat");
        }

        public VertexiumCypherScope build() {
            return newItemsScope(items, parentScope);
        }
    }
}
