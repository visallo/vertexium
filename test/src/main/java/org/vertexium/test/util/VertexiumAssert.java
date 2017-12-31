package org.vertexium.test.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.event.GraphEvent;
import org.vertexium.query.IterableWithTotalHits;
import org.vertexium.query.QueryResultsIterable;

import java.util.*;
import java.util.stream.Collectors;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.IterableUtils.toList;
import static org.vertexium.util.StreamUtils.stream;

public class VertexiumAssert {
    protected final static List<GraphEvent> graphEvents = new ArrayList<>();

    public static void assertIdsAnyOrder(Iterable<String> ids, String... expectedIds) {
        List<String> sortedIds = stream(ids).sorted().collect(Collectors.toList());
        Arrays.sort(expectedIds);

        String idsString = idsToString(sortedIds.toArray(new String[sortedIds.size()]));
        String expectedIdsString = idsToString(expectedIds);
        assertEquals("ids length mismatch found:[" + idsString + "] expected:[" + expectedIdsString + "]", expectedIds.length, sortedIds.size());
        for (int i = 0; i < expectedIds.length; i++) {
            assertEquals("at offset: " + i + " found:[" + idsString + "] expected:[" + expectedIdsString + "]", expectedIds[i], sortedIds.get(i));
        }
    }

    public static void assertVertexIdsAnyOrder(Iterable<Vertex> vertices, String... expectedIds) {
        List<Vertex> sortedVertices = stream(vertices)
                .sorted(Comparator.comparing(Element::getId))
                .collect(Collectors.toList());
        Arrays.sort(expectedIds);
        assertVertexIds(sortedVertices, expectedIds);
    }

    public static void assertVertexIds(Iterable<Vertex> vertices, String... expectedIds) {
        String verticesIdsString = vertexIdsToString(vertices);
        String expectedIdsString = idsToString(expectedIds);
        List<Vertex> verticesList = toList(vertices);
        assertEquals("ids length mismatch found:[" + verticesIdsString + "] expected:[" + expectedIdsString + "]", expectedIds.length, verticesList.size());
        for (int i = 0; i < expectedIds.length; i++) {
            assertEquals("at offset: " + i + " found:[" + verticesIdsString + "] expected:[" + expectedIdsString + "]", expectedIds[i], verticesList.get(i).getId());
        }
    }

    public static String vertexIdsToString(Iterable<Vertex> vertices) {
        List<String> idsList = stream(vertices)
                .map(Element::getId)
                .collect(Collectors.toList());
        String[] idsArray = idsList.toArray(new String[idsList.size()]);
        return idsToString(idsArray);
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

    public static void assertEdgeIdsAnyOrder(Iterable<Edge> edges, String... expectedIds) {
        List<Edge> sortedEdges = stream(edges)
                .sorted(Comparator.comparing(Element::getId))
                .collect(Collectors.toList());
        Arrays.sort(expectedIds);
        assertEdgeIds(sortedEdges, expectedIds);
    }

    public static void assertEdgeIds(Iterable<Edge> edges, String... ids) {
        List<Edge> edgesList = toList(edges);
        assertEquals("ids length mismatch", ids.length, edgesList.size());
        for (int i = 0; i < ids.length; i++) {
            assertEquals("at offset: " + i, ids[i], edgesList.get(i).getId());
        }
    }

    public static void assertResultsCount(int expectedCountAndTotalHits, QueryResultsIterable<? extends Element> results) {
        assertEquals(expectedCountAndTotalHits, results.getTotalHits());
        assertEquals(expectedCountAndTotalHits, count(results));
    }

    public static void assertResultsCount(
            int expectedCount,
            int expectedTotalHits,
            IterableWithTotalHits<?> results
    ) {
        assertEquals(expectedTotalHits, results.getTotalHits());
        assertEquals(expectedCount, count(results));
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
