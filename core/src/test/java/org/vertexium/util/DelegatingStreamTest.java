package org.vertexium.util;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.*;

import static org.junit.Assert.assertTrue;

public class DelegatingStreamTest {
    @Test
    public void testClose() {
        assertTryWithResourceClosesStream(s -> s);
    }

    @Test
    public void testFilter() {
        assertTryWithResourceClosesStream(s -> s.filter(ss -> ss.equals("a") || ss.equals("b")));
    }

    @Test
    public void testMap() {
        assertTryWithResourceClosesStream(s -> s.map(ss -> true));
    }

    @Test
    public void testMapToInt() {
        assertTryWithResourceClosesStream(s -> s.mapToInt(ss -> 1));
    }

    @Test
    public void testMapToLong() {
        assertTryWithResourceClosesStream(s -> s.mapToLong(ss -> 1));
    }

    @Test
    public void testMapToDouble() {
        assertTryWithResourceClosesStream(s -> s.mapToDouble(ss -> 1));
    }

    @Test
    public void testFlatMap() {
        assertTryWithResourceClosesStream(s -> s.flatMap(ss -> Stream.of(ss + "1", ss + "2")));
    }

    @Test
    public void testFlatMapToInt() {
        assertTryWithResourceClosesStream(s -> s.flatMapToInt(ss -> IntStream.of(1, 2)));
    }

    @Test
    public void testFlatMapToLong() {
        assertTryWithResourceClosesStream(s -> s.flatMapToLong(ss -> LongStream.of(1, 2)));
    }

    @Test
    public void testFlatMapToDouble() {
        assertTryWithResourceClosesStream(s -> s.flatMapToDouble(ss -> DoubleStream.of(1, 2)));
    }

    @Test
    public void testDistinct() {
        assertTryWithResourceClosesStream(Stream::distinct);
    }

    @Test
    public void testSorted() {
        assertTryWithResourceClosesStream(Stream::sorted);
    }

    @Test
    public void testSortedCompare() {
        assertTryWithResourceClosesStream(stringStream -> stringStream.sorted((o1, o2) -> -o1.compareTo(o2)));
    }

    @Test
    public void testPeek() {
        assertTryWithResourceClosesStream(stringStream -> stringStream.peek(s -> {
        }));
    }

    @Test
    public void testLimit() {
        assertTryWithResourceClosesStream(s -> s.limit(1));
    }

    @Test
    public void testSkip() {
        assertTryWithResourceClosesStream(s -> s.skip(1));
    }

    @Test
    public void testForEach() {
        assertTerminalOpCallsClose(s -> s.forEach(ss -> {
        }));
    }

    @Test
    public void testForEachOrdered() {
        assertTerminalOpCallsClose(s -> s.forEachOrdered(ss -> {
        }));
    }

    @Test
    public void testToArray() {
        assertTerminalOpCallsClose(s -> s.toArray());
    }

    @Test
    public void testToArrayInt() {
        assertTerminalOpCallsClose(s -> s.toArray(len -> new String[len]));
    }

    @Test
    public void testReduceWithIdentity() {
        assertTerminalOpCallsClose(s -> s.reduce("a", (str1, str2) -> str1 + str2));
    }

    @Test
    public void testReduce() {
        assertTerminalOpCallsClose(s -> s.reduce((str1, str2) -> str1 + str2));
    }

    @Test
    public void testReduceWithIdentityAndCombiner() {
        assertTerminalOpCallsClose(stream -> stream.reduce(1, (i, s) -> i + s.length(), (i1, i2) -> i1 + i2));
    }

    @Test
    public void testCollectWithSupplierAndCombiner() {
        assertTerminalOpCallsClose(stream -> stream.collect(
            () -> 1,
            (i, s) -> {
            },
            (i, s) -> {
            }
        ));
    }

    @Test
    public void testCollect() {
        assertTerminalOpCallsClose(stream -> stream.collect(Collectors.toList()));
    }

    @Test
    public void testMin() {
        assertTerminalOpCallsClose(stream -> stream.min(String::compareTo));
    }

    @Test
    public void testMax() {
        assertTerminalOpCallsClose(stream -> stream.max(String::compareTo));
    }

    @Test
    public void testCount() {
        assertTerminalOpCallsClose(stream -> stream.count());
    }

    @Test
    public void testAnyMatch() {
        assertTerminalOpCallsClose(stream -> stream.anyMatch(s -> true));
    }

    @Test
    public void testAllMatch() {
        assertTerminalOpCallsClose(stream -> stream.allMatch(s -> true));
    }

    @Test
    public void testNoneMatch() {
        assertTerminalOpCallsClose(stream -> stream.noneMatch(s -> true));
    }

    @Test
    public void testFindFirst() {
        assertTerminalOpCallsClose(s -> s.findFirst());
    }

    @Test
    public void testFindAny() {
        assertTerminalOpCallsClose(s -> s.findAny());
    }

    @Test
    public void testIterator() {
        assertTerminalOpCallsClose(s -> {
            Iterator<String> it = s.iterator();
            while (it.hasNext()) {
                it.next();
            }
        });
    }

    @Test
    public void testSpliterator() {
        assertTerminalOpCallsClose(s -> {
            Spliterator<String> split = s.spliterator();
            while (split.tryAdvance(z -> {

            })) {

            }
        });
    }

    @Test
    public void testSequential() {
        assertTryWithResourceClosesStream(s -> s.sequential());
    }

    @Test
    public void testParallel() {
        assertTryWithResourceClosesStream(s -> s.parallel());
    }

    @Test
    public void testUnordered() {
        assertTryWithResourceClosesStream(s -> s.unordered());
    }

    @Test
    public void testNestedClose() throws InterruptedException {
        AtomicBoolean closeCalled = new AtomicBoolean(false);
        Set<String> results = new TestStream(closeCalled, "a", "b", "c")
            .map(String::toUpperCase)
            .collect(Collectors.toSet());
        System.out.println(results.toString());
        results = null;
        for (int i = 0; i < 10 && !closeCalled.get(); i++) {
            System.gc();
            Thread.sleep(10);
        }
        assertTrue(closeCalled.get());
    }

    @Test
    public void testOnClose() {
        AtomicBoolean closed = new AtomicBoolean(false);
        assertTryWithResourceClosesStream(s -> s.onClose(() -> {
            closed.set(true);
        }));
        assertTrue(closed.get());
    }

    private void assertTerminalOpCallsClose(Consumer<Stream<String>> fn) {
        AtomicBoolean closeCalled = new AtomicBoolean(false);
        fn.accept(new TestStream(closeCalled, "a", "b", "c"));
        assertTrue(closeCalled.get());
    }

    private void assertTryWithResourceClosesStream(Function<Stream<String>, BaseStream> fn) {
        AtomicBoolean closeCalled = new AtomicBoolean(false);
        try (BaseStream stream = fn.apply(new TestStream(closeCalled, "a", "b", "c"))) {
        }
        assertTrue(closeCalled.get());
    }

    private static class TestStream extends DelegatingStream<String> {
        private final AtomicBoolean closeCalled;

        public TestStream(AtomicBoolean closeCalled, String... items) {
            super(Lists.newArrayList(items).stream());
            this.closeCalled = closeCalled;
        }

        @Override
        public void close() {
            super.close();
            closeCalled.set(true);
        }
    }
}