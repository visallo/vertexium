package org.vertexium.cypher;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class VertexiumCypherResult implements Stream<CypherResultRow> {
    private final Stream<CypherResultRow> stream;
    private final LinkedHashSet<String> columnNames;

    public VertexiumCypherResult(Stream<CypherResultRow> stream, LinkedHashSet<String> columnNames) {
        this.stream = stream;
        this.columnNames = columnNames;
    }

    public LinkedHashSet<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public Iterator<CypherResultRow> iterator() {
        return stream.iterator();
    }

    @Override
    public Spliterator<CypherResultRow> spliterator() {
        return stream.spliterator();
    }

    @Override
    public boolean isParallel() {
        return stream.isParallel();
    }

    @Override
    public Stream<CypherResultRow> sequential() {
        return stream.sequential();
    }

    @Override
    public Stream<CypherResultRow> parallel() {
        return stream.parallel();
    }

    @Override
    public Stream<CypherResultRow> unordered() {
        return stream.unordered();
    }

    @Override
    public Stream<CypherResultRow> onClose(Runnable closeHandler) {
        return stream.onClose(closeHandler);
    }

    @Override
    public void close() {
        stream.close();
    }

    @Override
    public VertexiumCypherResult filter(Predicate<? super CypherResultRow> predicate) {
        return new VertexiumCypherResult(
            stream.filter(predicate),
            getColumnNames()
        );
    }

    @Override
    public <R> Stream<R> map(Function<? super CypherResultRow, ? extends R> mapper) {
        return stream.map(mapper);
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super CypherResultRow> mapper) {
        return stream.mapToInt(mapper);
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super CypherResultRow> mapper) {
        return stream.mapToLong(mapper);
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super CypherResultRow> mapper) {
        return stream.mapToDouble(mapper);
    }

    @Override
    public <R> Stream<R> flatMap(Function<? super CypherResultRow, ? extends Stream<? extends R>> mapper) {
        return stream.flatMap(mapper);
    }

    public VertexiumCypherResult flatMapCypherResult(Function<? super CypherResultRow, ? extends Stream<? extends CypherResultRow>> mapper) {
        return new VertexiumCypherResult(
            stream.flatMap(mapper),
            getColumnNames()
        );
    }

    @Override
    public IntStream flatMapToInt(Function<? super CypherResultRow, ? extends IntStream> mapper) {
        return stream.flatMapToInt(mapper);
    }

    @Override
    public LongStream flatMapToLong(Function<? super CypherResultRow, ? extends LongStream> mapper) {
        return stream.flatMapToLong(mapper);
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super CypherResultRow, ? extends DoubleStream> mapper) {
        return stream.flatMapToDouble(mapper);
    }

    @Override
    public VertexiumCypherResult distinct() {
        return new VertexiumCypherResult(
            stream.distinct(),
            getColumnNames()
        );
    }

    @Override
    public Stream<CypherResultRow> sorted() {
        return stream.sorted();
    }

    @Override
    public VertexiumCypherResult sorted(Comparator<? super CypherResultRow> comparator) {
        return new VertexiumCypherResult(
            stream.sorted(comparator),
            this.getColumnNames()
        );
    }

    @Override
    public VertexiumCypherResult peek(Consumer<? super CypherResultRow> action) {
        return new VertexiumCypherResult(
            stream.peek(action),
            this.getColumnNames()
        );
    }

    @Override
    public Stream<CypherResultRow> limit(long maxSize) {
        return stream.limit(maxSize);
    }

    @Override
    public Stream<CypherResultRow> skip(long n) {
        return stream.skip(n);
    }

    @Override
    public void forEach(Consumer<? super CypherResultRow> action) {
        stream.forEach(action);
    }

    @Override
    public void forEachOrdered(Consumer<? super CypherResultRow> action) {
        stream.forEachOrdered(action);
    }

    @Override
    public Object[] toArray() {
        return stream.toArray();
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        return stream.toArray(generator);
    }

    @Override
    public CypherResultRow reduce(CypherResultRow identity, BinaryOperator<CypherResultRow> accumulator) {
        return stream.reduce(identity, accumulator);
    }

    @Override
    public Optional<CypherResultRow> reduce(BinaryOperator<CypherResultRow> accumulator) {
        return stream.reduce(accumulator);
    }

    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super CypherResultRow, U> accumulator, BinaryOperator<U> combiner) {
        return stream.reduce(identity, accumulator, combiner);
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super CypherResultRow> accumulator, BiConsumer<R, R> combiner) {
        return stream.collect(supplier, accumulator, combiner);
    }

    @Override
    public <R, A> R collect(Collector<? super CypherResultRow, A, R> collector) {
        return stream.collect(collector);
    }

    @Override
    public Optional<CypherResultRow> min(Comparator<? super CypherResultRow> comparator) {
        return stream.min(comparator);
    }

    @Override
    public Optional<CypherResultRow> max(Comparator<? super CypherResultRow> comparator) {
        return stream.max(comparator);
    }

    @Override
    public long count() {
        return stream.count();
    }

    @Override
    public boolean anyMatch(Predicate<? super CypherResultRow> predicate) {
        return stream.anyMatch(predicate);
    }

    @Override
    public boolean allMatch(Predicate<? super CypherResultRow> predicate) {
        return stream.allMatch(predicate);
    }

    @Override
    public boolean noneMatch(Predicate<? super CypherResultRow> predicate) {
        return stream.noneMatch(predicate);
    }

    @Override
    public Optional<CypherResultRow> findFirst() {
        return stream.findFirst();
    }

    @Override
    public Optional<CypherResultRow> findAny() {
        return stream.findAny();
    }

    public static <T> Builder<T> builder() {
        return Stream.builder();
    }

    public static <T> Stream<T> empty() {
        return Stream.empty();
    }

    public static <T> Stream<T> of(T t) {
        return Stream.of(t);
    }

    @SafeVarargs
    public static <T> Stream<T> of(T... values) {
        return Stream.of(values);
    }

    public static <T> Stream<T> iterate(T seed, UnaryOperator<T> f) {
        return Stream.iterate(seed, f);
    }

    public static <T> Stream<T> generate(Supplier<T> s) {
        return Stream.generate(s);
    }

    public static <T> Stream<T> concat(Stream<? extends T> a, Stream<? extends T> b) {
        return Stream.concat(a, b);
    }
}
