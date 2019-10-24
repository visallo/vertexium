package org.vertexium.test.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.event.GraphEvent;
import org.vertexium.metric.StackTraceTracker;
import org.vertexium.query.IterableWithTotalHits;
import org.vertexium.query.QueryResultsIterable;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;
import static org.vertexium.util.StreamUtils.stream;

public class VertexiumAssert {
    protected final static List<GraphEvent> graphEvents = new ArrayList<>();

    public static void assertIdsAnyOrder(Iterable<String> ids, String... expectedIds) {
        List<String> sortedIds = stream(ids).sorted().collect(Collectors.toList());
        Arrays.sort(expectedIds);

        String idsString = idsToString(sortedIds.toArray(new String[0]));
        String expectedIdsString = idsToString(expectedIds);
        assertEquals("ids length mismatch found:[" + idsString + "] expected:[" + expectedIdsString + "]", expectedIds.length, sortedIds.size());
        for (int i = 0; i < expectedIds.length; i++) {
            assertEquals("at offset: " + i + " found:[" + idsString + "] expected:[" + expectedIdsString + "]", expectedIds[i], sortedIds.get(i));
        }
    }

    public static void assertVertexIdsAnyOrder(Iterable<? extends Vertex> vertices, String... expectedIds) {
        assertVertexiumObjectIdsAnyOrder(vertices, (Object[]) expectedIds);
    }

    public static void assertEdgeIdsAnyOrder(Iterable<? extends Edge> edges, String... expectedIds) {
        assertVertexiumObjectIdsAnyOrder(edges, (Object[]) expectedIds);
    }

    public static void assertElementIdsAnyOrder(Iterable<? extends Element> elements, String... expectedIds) {
        assertVertexiumObjectIdsAnyOrder(elements, (Object[]) expectedIds);
    }

    public static void assertVertexIds(Iterable<? extends Vertex> vertices, String... expectedIds) {
        assertVertexiumObjectIds(vertices, (Object[]) expectedIds);
    }

    public static void assertEdgeIds(Iterable<? extends Edge> edges, String... expectedIds) {
        assertVertexiumObjectIds(edges, (Object[]) expectedIds);
    }

    public static void assertElementIds(Iterable<? extends Element> elements, String... expectedIds) {
        assertVertexiumObjectIds(elements, (Object[]) expectedIds);
    }

    public static String idsToString(String[] ids) {
        return Joiner.on(", ").join(ids);
    }

    public static String idsToStringSorted(Iterable<String> ids) {
        ArrayList<String> idsList = Lists.newArrayList(ids);
        Collections.sort(idsList);
        return Joiner.on(", ").join(idsList);
    }

    public static void assertEvents(GraphEvent... expectedEvents) {
        assertEquals("Different number of events occurred than were asserted", expectedEvents.length, graphEvents.size());

        for (int i = 0; i < expectedEvents.length; i++) {
            assertEquals(expectedEvents[i], graphEvents.get(i));
        }
    }

    public static void assertVertexiumObjectIdsAnyOrder(Iterable<? extends VertexiumObject> vertexiumObjects, Object... ids) {
        if (vertexiumObjects instanceof QueryResultsIterable) {
            assertEquals(ids.length, ((QueryResultsIterable) vertexiumObjects).getTotalHits());
        }

        Comparator<Object> idCompare = (o1, o2) -> {
            if (o1 instanceof String && o2 instanceof String) {
                return ((String) o1).compareTo((String) o2);
            }
            throw new VertexiumException("Unhandled compare");
        };
        List<Object> foundIds = stream(vertexiumObjects)
            .map(VertexiumObject::getId)
            .sorted(idCompare)
            .collect(Collectors.toList());
        List<Object> expectedIds = Arrays.stream(ids)
            .sorted(idCompare)
            .collect(Collectors.toList());
        assertEquals(
            Joiner.on(", ").join(expectedIds),
            Joiner.on(", ").join(foundIds)
        );
    }

    public static void assertVertexiumObjectIds(Iterable<? extends VertexiumObject> vertexiumObjects, Object... ids) {
        if (vertexiumObjects instanceof QueryResultsIterable) {
            assertEquals(ids.length, ((QueryResultsIterable) vertexiumObjects).getTotalHits());
        }

        List<Object> foundIds = stream(vertexiumObjects)
            .map(VertexiumObject::getId)
            .collect(Collectors.toList());
        List<Object> expectedIds = Arrays.stream(ids)
            .collect(Collectors.toList());
        assertEquals(
            Joiner.on(", ").join(expectedIds),
            Joiner.on(", ").join(foundIds)
        );
    }

    public static void assertResultsCount(int expectedCountAndTotalHits, QueryResultsIterable<? extends Element> results) {
        assertEquals(expectedCountAndTotalHits, results.getTotalHits());
        assertCount(expectedCountAndTotalHits, results);
    }

    public static void assertResultsCount(
        int expectedCount,
        int expectedTotalHits,
        IterableWithTotalHits<?> results
    ) {
        assertEquals("total hits", expectedTotalHits, results.getTotalHits());
        assertCount(expectedCount, results);
    }

    private static void assertCount(int expectedCount, Iterable<?> results) {
        int count = 0;
        Iterator<?> it = results.iterator();
        while (it.hasNext()) {
            count++;
            it.next();
        }
        assertEquals("count", expectedCount, count);
        assertFalse(it.hasNext());
        try {
            it.next();
            throw new VertexiumException("Should throw NoSuchElementException: " + it.getClass().getName());
        } catch (NoSuchElementException ex) {
            // OK
        }
    }

    public static void addGraphEvent(GraphEvent graphEvent) {
        graphEvents.add(graphEvent);
    }

    public static void clearGraphEvents() {
        graphEvents.clear();
    }

    @SuppressWarnings("unchecked")
    public static <T> void assertSet(Set<T> set, T... values) {
        assertEquals("size mismatch", values.length, set.size());
        for (T value : values) {
            assertTrue("set missing " + value, set.contains(value));
        }
    }

    public static void assertRowIdsAnyOrder(Iterable<ExtendedDataRow> rows, String... expectedRowIds) {
        List<String> foundRowIds = getRowIds(rows);
        assertEquals(idsToStringSorted(Lists.newArrayList(expectedRowIds)), idsToStringSorted(foundRowIds));
        if (rows instanceof QueryResultsIterable) {
            assertEquals(
                "search index total hits mismatch",
                expectedRowIds.length,
                ((QueryResultsIterable<ExtendedDataRow>) rows).getTotalHits()
            );
        }
    }

    public static void assertRowIdsAnyOrder(Iterable<String> expectedRowIds, Iterable<? extends VertexiumObject> searchResults) {
        List<String> foundRowIds = getRowIds(searchResults);
        assertEquals(idsToStringSorted(expectedRowIds), idsToStringSorted(foundRowIds));
    }

    public static void assertRowIds(Iterable<String> expectedRowIds, Iterable<? extends VertexiumObject> searchResults) {
        List<String> foundRowIds = getRowIds(searchResults);
        assertEquals(expectedRowIds, foundRowIds);
    }

    private static List<String> getRowIds(Iterable<? extends VertexiumObject> searchResults) {
        return stream(searchResults)
            .filter((sr) -> sr instanceof ExtendedDataRow)
            .map((sr) -> ((ExtendedDataRow) sr).getId().getRowId())
            .collect(Collectors.toList());
    }

    public static void assertThrowsException(Runnable fn) {
        try {
            fn.run();
        } catch (Throwable ex) {
            return;
        }
        fail("Should have thrown an exception");
    }

    public static void assertStackTraceTrackerCount(StackTraceTracker tracker, Consumer<List<StackTraceTracker.StackTraceItem>> validate) {
        assertStackTraceTrackerCount(tracker.getRoots(), new ArrayList<>(), validate);
    }

    public static void assertStackTraceTrackerCount(
        Set<StackTraceTracker.StackTraceItem> roots,
        List<StackTraceTracker.StackTraceItem> path,
        Consumer<List<StackTraceTracker.StackTraceItem>> validate
    ) {
        for (StackTraceTracker.StackTraceItem item : roots) {
            List<StackTraceTracker.StackTraceItem> newPath = new ArrayList<>(path);
            newPath.add(item);
            validate.accept(newPath);
            assertStackTraceTrackerCount(item.getChildren(), newPath, validate);
        }
    }
}
