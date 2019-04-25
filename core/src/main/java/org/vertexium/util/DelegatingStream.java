package org.vertexium.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.*;
import java.util.stream.*;

public class DelegatingStream<T> implements Stream<T> {
    private final Stream<T> stream;
    private boolean streamCloseCalled;

    public DelegatingStream(Stream<T> stream) {
        this.stream = stream.onClose(this::close);
    }

    @Override
    public Stream<T> filter(Predicate<? super T> predicate) {
        return stream.filter(predicate);
    }

    @Override
    public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
        return stream.map(mapper);
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super T> mapper) {
        return stream.mapToInt(mapper);
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super T> mapper) {
        return stream.mapToLong(mapper);
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        return stream.mapToDouble(mapper);
    }

    @Override
    public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return stream.flatMap(mapper);
    }

    @Override
    public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        return stream.flatMapToInt(mapper);
    }

    @Override
    public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        return stream.flatMapToLong(mapper);
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        return stream.flatMapToDouble(mapper);
    }

    @Override
    public Stream<T> distinct() {
        return stream.distinct();
    }

    @Override
    public Stream<T> sorted() {
        return stream.sorted();
    }

    @Override
    public Stream<T> sorted(Comparator<? super T> comparator) {
        return stream.sorted(comparator);
    }

    @Override
    public Stream<T> peek(Consumer<? super T> action) {
        return stream.peek(action);
    }

    @Override
    public Stream<T> limit(long maxSize) {
        return stream.limit(maxSize);
    }

    @Override
    public Stream<T> skip(long n) {
        return stream.skip(n);
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        stream.forEach(action);
        close();
    }

    @Override
    public void forEachOrdered(Consumer<? super T> action) {
        stream.forEachOrdered(action);
        close();
    }

    @Override
    public Object[] toArray() {
        Object[] arr = stream.toArray();
        close();
        return arr;
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        A[] arr = stream.toArray(generator);
        close();
        return arr;
    }

    @Override
    public T reduce(T identity, BinaryOperator<T> accumulator) {
        T reduce = stream.reduce(identity, accumulator);
        close();
        return reduce;
    }

    @Override
    public Optional<T> reduce(BinaryOperator<T> accumulator) {
        Optional<T> reduce = stream.reduce(accumulator);
        close();
        return reduce;
    }

    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
        U reduce = stream.reduce(identity, accumulator, combiner);
        close();
        return reduce;
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        R collect = stream.collect(supplier, accumulator, combiner);
        close();
        return collect;
    }

    @Override
    public <R, A> R collect(Collector<? super T, A, R> collector) {
        R collect = stream.collect(collector);
        close();
        return collect;
    }

    @Override
    public Optional<T> min(Comparator<? super T> comparator) {
        Optional<T> min = stream.min(comparator);
        close();
        return min;
    }

    @Override
    public Optional<T> max(Comparator<? super T> comparator) {
        Optional<T> max = stream.max(comparator);
        close();
        return max;
    }

    @Override
    public long count() {
        long count = stream.count();
        close();
        return count;
    }

    @Override
    public boolean anyMatch(Predicate<? super T> predicate) {
        boolean b = stream.anyMatch(predicate);
        close();
        return b;
    }

    @Override
    public boolean allMatch(Predicate<? super T> predicate) {
        boolean b = stream.allMatch(predicate);
        close();
        return b;
    }

    @Override
    public boolean noneMatch(Predicate<? super T> predicate) {
        boolean b = stream.noneMatch(predicate);
        close();
        return b;
    }

    @Override
    public Optional<T> findFirst() {
        Optional<T> first = stream.findFirst();
        close();
        return first;
    }

    @Override
    public Optional<T> findAny() {
        Optional<T> any = stream.findAny();
        close();
        return any;
    }

    @Override
    public Iterator<T> iterator() {
        Iterator<T> it = stream.iterator();
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                boolean hasNext = it.hasNext();
                if (!hasNext) {
                    close();
                }
                return hasNext;
            }

            @Override
            public T next() {
                return it.next();
            }
        };
    }

    @Override
    public Spliterator<T> spliterator() {
        Spliterator<T> split = stream.spliterator();
        return new Spliterator<T>() {
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                boolean b = split.tryAdvance(action);
                if (!b) {
                    close();
                }
                return b;
            }

            @Override
            public Spliterator<T> trySplit() {
                return split.trySplit();
            }

            @Override
            public long estimateSize() {
                return split.estimateSize();
            }

            @Override
            public int characteristics() {
                return split.characteristics();
            }
        };
    }

    @Override
    public boolean isParallel() {
        return stream.isParallel();
    }

    @Override
    public Stream<T> sequential() {
        return stream.sequential();
    }

    @Override
    public Stream<T> parallel() {
        return stream.parallel();
    }

    @Override
    public Stream<T> unordered() {
        return stream.unordered();
    }

    @Override
    public Stream<T> onClose(Runnable closeHandler) {
        return stream.onClose(closeHandler);
    }

    @Override
    public void close() {
        if (!streamCloseCalled) {
            stream.close();
            streamCloseCalled = true;
        }
    }
}
