package org.vertexium.test.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.query.IterableWithTotalHits;
import org.vertexium.query.QueryResultsIterable;

import java.util.*;
import java.util.stream.Collectors;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;
import static org.vertexium.util.StreamUtils.stream;

public class VertexiumAssert {
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
        assertElementIdsAnyOrder(vertices, expectedIds);
        if (vertices instanceof QueryResultsIterable) {
            assertEquals("total hits", expectedIds.length, ((QueryResultsIterable<Vertex>) vertices).getTotalHits());
        }
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
        String found = stream(elements).map(ElementId::getId).collect(Collectors.joining(", "));
        String expected = Joiner.on(", ").join(ids);
        assertEquals(expected, found);
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

    public static void assertMetadata(Object expectedValue, Element element, String propertyName, String metadataName) {
        Property property = element.getProperty(propertyName);
        assertNotNull("Could not find property: " + propertyName + " on element " + element, property);
        Metadata.Entry metadataEntry = property.getMetadata().getEntry(metadataName);
        assertNotNull("Could not find metadata entry: " + metadataName + " on property: " + propertyName + " on element " + element, metadataEntry);
        assertEquals(expectedValue, metadataEntry.getValue());
    }
}
