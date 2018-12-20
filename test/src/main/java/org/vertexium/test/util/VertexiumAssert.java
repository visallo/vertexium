package org.vertexium.test.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.event.GraphEvent;
import org.vertexium.query.IterableWithTotalHits;
import org.vertexium.query.QueryResultsIterable;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.vertexium.util.IterableUtils.toList;
import static org.vertexium.util.StreamUtils.stream;

public class VertexiumAssert {
    protected final static List<GraphEvent> graphEvents = new ArrayList<>();

    public static void assertIdsAnyOrder(Iterable<String> ids, String... expectedIds) {
        List<String> sortedIds = stream(ids).sorted().collect(Collectors.toList());
        Arrays.sort(expectedIds);

        String idsString = idsToString(sortedIds.toArray(new String[sortedIds.size()]));
        String expectedIdsString = idsToString(expectedIds);
        assertEquals(expectedIds.length, sortedIds.size(), "ids length mismatch found:[" + idsString + "] expected:[" + expectedIdsString + "]");
        for (int i = 0; i < expectedIds.length; i++) {
            assertEquals(expectedIds[i], sortedIds.get(i), "at offset: " + i + " found:[" + idsString + "] expected:[" + expectedIdsString + "]");
        }
    }

    public static void assertVertexIdsAnyOrder(Iterable<Vertex> vertices, String... expectedIds) {
        assertElementIdsAnyOrder(vertices, expectedIds);
    }

    public static void assertVertexIds(Iterable<Vertex> vertices, String... expectedIds) {
        assertElementIds(vertices, expectedIds);
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
        assertEquals(expectedEvents.length, graphEvents.size(), "Different number of events occurred than were asserted");

        for (int i = 0; i < expectedEvents.length; i++) {
            assertEquals(expectedEvents[i], graphEvents.get(i));
        }
    }

    public static void assertEdgeIdsAnyOrder(Iterable<Edge> edges, String... expectedIds) {
        assertElementIdsAnyOrder(edges, expectedIds);
    }

    public static void assertEdgeIds(Iterable<Edge> edges, String... ids) {
        assertElementIds(edges, ids);
    }

    public static void assertElementIdsAnyOrder(Iterable<? extends Element> elements, String... expectedIds) {
        List<Element> sortedElements = stream(elements)
                .sorted(Comparator.comparing(Element::getId))
                .collect(Collectors.toList());
        Arrays.sort(expectedIds);
        assertElementIds(sortedElements, expectedIds);
    }

    public static void assertElementIds(Iterable<? extends Element> elements, String... ids) {
        List<Element> elementList = toList(elements);
        assertEquals(ids.length, elementList.size(), "ids length mismatch");
        for (int i = 0; i < ids.length; i++) {
            assertEquals(ids[i], elementList.get(i).getId(), "at offset: " + i);
        }
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
        assertEquals(expectedTotalHits, results.getTotalHits());
        assertCount(expectedCount, results);
    }

    private static void assertCount(int expectedCount, Iterable<?> results) {
        int count = 0;
        Iterator<?> it = results.iterator();
        while (it.hasNext()) {
            count++;
            it.next();
        }
        assertEquals(expectedCount, count);
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
}
