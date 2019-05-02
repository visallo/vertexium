package org.vertexium.accumulo.iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.accumulo.core.client.impl.BaseIteratorEnvironment;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.commons.collections.BufferOverflowException;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class RowDeduplicationIteratorTest {

    private static final class DummyIteratorEnv extends BaseIteratorEnvironment {
        @Override
        public IteratorUtil.IteratorScope getIteratorScope() {
            return IteratorUtil.IteratorScope.scan;
        }

        @Override
        public boolean isFullMajorCompaction() {
            return false;
        }
    }

    private void pkv(SortedMap<Key, Value> map, String row, String cf, String cq, String cv, long ts,
                     byte[] val) {
        map.put(new Key(new Text(row), new Text(cf), new Text(cq), new Text(cv), ts),
                new Value(val, true));
    }

    @Test
    public void testSimpleRows() throws IOException {
        byte[] kbVal = new byte[1024];
        // This code is shamelessly borrowed from the WholeRowIteratorTest.
        SortedMap<Key, Value> map1 = new TreeMap<>();
        //Build each of the rows to resemble our scenario - each row should have multiple properties
        pkv(map1, "row1", "EIN", "name", "cv1", 1, "ein_stuff_row1".getBytes());
        pkv(map1, "row1", "PROP", "name", "cv1", 2, "red".getBytes());
        pkv(map1, "row1", "PROP", "name", "cv1", 3, "red".getBytes());
        pkv(map1, "row1", "PROP", "name", "cv1", 4, "yellow".getBytes());
        pkv(map1, "row1", "PROPD", "name", "cv1", 5, "".getBytes());
        pkv(map1, "row1", "PROPD", "name", "cv1", 6, "".getBytes());
        pkv(map1, "row1", "PROPMETA", "name", "cv1", 2, "red".getBytes());
        pkv(map1, "row1", "PROPMETA", "name", "cv1", 3, "red".getBytes());
        pkv(map1, "row1", "PROPMETA", "name", "cv1", 4, "yellow".getBytes());
        pkv(map1, "row1", "V", "name", "cv1", 7, "".getBytes());

        SortedMap<Key, Value> map2 = new TreeMap<>();
        pkv(map2, "row2", "EIN", "name", "cv1", 1, "ein_stuff_row2".getBytes());
        pkv(map2, "row2", "PROP", "name", "cv1", 2, "red".getBytes());
        pkv(map2, "row2", "PROP", "name", "cv1", 3, "blue".getBytes());
        pkv(map2, "row2", "PROP", "name", "cv1", 4, "yellow".getBytes());
        pkv(map2, "row2", "PROPD", "name", "cv1", 5, "".getBytes());
        pkv(map2, "row2", "PROPD", "name", "cv1", 6, "".getBytes());
        pkv(map2, "row2", "PROPMETA", "name", "cv1", 2, "red".getBytes());
        pkv(map2, "row2", "PROPMETA", "name", "cv1", 3, "blue".getBytes());
        pkv(map2, "row2", "PROPMETA", "name", "cv1", 4, "yellow".getBytes());
        pkv(map2, "row2", "V", "name", "cv1", 7, "".getBytes());

        SortedMap<Key, Value> map3 = new TreeMap<>();
        pkv(map3, "row3", "EIN", "name", "cv1", 1, "ein_stuff_row2".getBytes());
        pkv(map3, "row3", "PROP", "name", "cv1", 2, "red".getBytes());
        pkv(map3, "row3", "PROP", "name", "cv1", 3, "red".getBytes());
        pkv(map3, "row3", "PROP", "name", "cv1", 4, "red".getBytes());
        pkv(map3, "row3", "PROPD", "name", "cv1", 5, "".getBytes());
        pkv(map3, "row3", "PROPD", "name", "cv1", 8, "".getBytes());
        pkv(map3, "row3", "PROPMETA", "name", "cv1", 2, "red".getBytes());
        pkv(map3, "row3", "PROPMETA", "name", "cv1", 3, "red".getBytes());
        pkv(map3, "row3", "PROPMETA", "name", "cv1", 4, "red".getBytes());
        pkv(map3, "row3", "V", "name", "cv1", 7, "".getBytes());

        SortedMap<Key, Value> map = new TreeMap<>();
        map.putAll(map1);
        map.putAll(map2);
        map.putAll(map3);
        SortedMapIterator src = new SortedMapIterator(map);
        //Range range = new Range(new Text("row1"), true, new Text("row2"), true);
        Range range = new Range();
        RowDeduplicationIterator iter = new RowDeduplicationIterator();
        Map<String, String> bigBufferOpts = new HashMap<>();
        bigBufferOpts.put(RowDeduplicationIterator.MAX_BUFFER_SIZE_OPT, "3K");
        iter.init(src, bigBufferOpts, new DummyIteratorEnv());
        iter.seek(range, new ArrayList<>(), false);

        TreeMap<Key, Value> tmOut = new TreeMap<>();
        while (iter.hasTop()) {
            tmOut.put(iter.getTopKey(), iter.getTopValue());
            iter.next();
        }
        assertTrue(tmOut.size() == 22);
        List<Long> row1ActualOrder = new ArrayList<>();
        List<Long> row2ActualOrder = new ArrayList<>();
        List<Long> row3ActualOrder = new ArrayList<>();
        for (Map.Entry<Key, Value> entry : tmOut.entrySet()) {
            if (entry.getKey().getRow().toString().equalsIgnoreCase("row1")) {
                row1ActualOrder.add(entry.getKey().getTimestamp());
            }
            if (entry.getKey().getRow().toString().equalsIgnoreCase("row2")) {
                row2ActualOrder.add(entry.getKey().getTimestamp());
            }
            if (entry.getKey().getRow().toString().equalsIgnoreCase("row3")) {
                row3ActualOrder.add(entry.getKey().getTimestamp());
            }
        }
        List<Long> row1ExpectedOrder = Arrays.asList(1L, 4L, 2L, 5L, 4L, 2L, 7L);
        assertTrue(row1ActualOrder.equals(row1ExpectedOrder));
        List<Long> row2ExpectedOrder = Arrays.asList(1L, 4L, 3L, 2L, 5L, 4L, 3L, 2L, 7L);
        assertTrue(row2ActualOrder.equals(row2ExpectedOrder));
        List<Long> row3ExpectedOrder = Arrays.asList(1L, 2L, 8L, 5L, 2L, 7L);
        assertTrue(row3ActualOrder.equals(row3ExpectedOrder));
    }

    @Test
    public void testLargeRow() throws IOException {
        byte[] kbVal = new byte[1024];
        // This code is shamelessly borrowed from the WholeRowIteratorTest.
        SortedMap<Key, Value> map = new TreeMap<>();
        //Build each of the rows to resemble our scenario - each row should have multiple properties
        pkv(map, "row1", "EIN", "qualifier_row1", "(workspace)|visallo", 1554913467261L, "shorty".getBytes());
        pkv(map, "row1", "EINH", "qualifier_row1", "workspace", 1554913892524L,  "ein_stuff_row1".getBytes());
        pkv(map, "row1", "PROP", "conceptType", "", 1554913466925L, "01http://visallo.org/broad#Person".getBytes());
        pkv(map, "row1", "PROP", "modifiedBy", "", 1554913466928L, "01USER_61e83306f89d40b296e3f6315b01bb67".getBytes());
        pkv(map, "row1", "PROP", "modifiedDate", "", 1554913466927L, "03x00x00x01jx08x10x9A".getBytes());
        pkv(map, "row1", "PROP", "visibilityJson", "", 1554913611933L, "x01{source:}".getBytes());
        pkv(map, "row1", "PROP", "visibilityJson", "", 1554913466926L, "x01{source:}".getBytes());
        pkv(map, "row1", "PROP", "name", "", 1554914059425L, "x01blue".getBytes());
        pkv(map, "row1", "PROP", "name", "", 1554913611934L, " x01red".getBytes());
        pkv(map, "row1", "PROP", "name", "((workspace))|visallo", 1554914017255L, "x01blue".getBytes());
        pkv(map, "row1", "PROP", "name", "((workspace))|visallo", 1554913825217L, "x01red".getBytes());
        pkv(map, "row1", "PROP", "name", "((workspace))|visallo", 1554913775169L, "x01blue".getBytes());
        pkv(map, "row1", "PROP", "name", "((workspace))|visallo", 1554913479614L, "x01red".getBytes());
        pkv(map, "row1", "PROPD", "name", "((workspace))|visallo", 1554914059427L, "".getBytes());
        pkv(map, "row1", "PROPD", "name", "((workspace))|visallo", 1554913892451L, "".getBytes());
        pkv(map, "row1", "PROPD", "name", "((workspace))|visallo", 1554913611937L, "".getBytes());
        pkv(map, "row1", "PROPH", "conceptyTypex1F", "workspace", 1554913965967L, "X".getBytes());
        pkv(map, "row1", "PROPH", "conceptyTypex1F", "workspace", 1554913965817L, "X".getBytes());
        pkv(map, "row1", "PROPH", "conceptyTypex1F", "workspace", 1554913892353L, "X".getBytes());
        pkv(map, "row1", "PROPH", "modifiedBy", "workspace", 1554913965968L, "X".getBytes());
        pkv(map, "row1", "PROPH", "modifiedBy", "workspace", 1554913965818L, "X".getBytes());
        pkv(map, "row1", "PROPH", "modifiedBy", "workspace", 1554913892367L, "X".getBytes());
        pkv(map, "row1", "PROPH", "modifiedDate", "workspace", 1554913965969L, "X".getBytes());
        pkv(map, "row1", "PROPH", "modifiedDate", "workspace", 1554913965819L, "X".getBytes());
        pkv(map, "row1", "PROPH", "modifiedDate", "workspace", 1554913892401L, "X".getBytes());
        pkv(map, "row1", "PROPH", "name", "workspace", 1554913965970L, "X".getBytes());
        pkv(map, "row1", "PROPH", "visibilityJson", "workspace", 1554913965820L, "X".getBytes());
        pkv(map, "row1", "PROPH", "visibilityJson", "workspace", 1554913892418L, "X".getBytes());
        pkv(map, "row1", "PROPH", "name", "workspace", 1554914059424L, "X".getBytes());
        pkv(map, "row1", "PROPH", "name", "workspace", 1554913965971L, "X".getBytes());
        pkv(map, "row1", "PROPH", "name", "workspace", 1554913965821L, "X".getBytes());
        pkv(map, "row1", "PROPH", "name", "workspace", 1554913775069L, "X".getBytes());
        pkv(map, "row1", "PROPMETA", "ame-confidence", "", 1554914059425L, "x04?xE0x00x00x00x00x00x00".getBytes());
        pkv(map, "row1", "PROPMETA", "ame-confidence", "", 1554913611934L, "x04?xE0x00x00x00x00x00x00".getBytes());
        pkv(map, "row1", "PROPMETA", "namex1Fhttp://visallo.org#modifiedBy", "", 1554914059425L, " x01USER_61e83306f89d40b296e3f6315b01bb67".getBytes());
        pkv(map, "row1", "PROPMETA", "namex1Fhttp://visallo.org#modifiedBy", "", 1554913611934L, " x01USER_61e83306f89d40b296e3f6315b01bb67".getBytes());
        pkv(map, "row1", "PROPMETA", "namex1Fhttp://visallo.org#modifiedDate", "", 1554914059425L, "x03x00x00x01jx08x18xFFxC2".getBytes());
        pkv(map, "row1", "PROPMETA", "namex1Fhttp://visallo.org#modifiedDate", "", 1554913611934L, "x03x00x00x01jx08x10xCBxBE".getBytes());
        pkv(map, "row1", "PROPMETA", "namex1Fhttp://visallo.org#visibilityJson ", "", 1554914059425L, "shorty".getBytes());
        pkv(map, "row1", "PROPMETA", "namex1Fhttp://visallo.org#visibilityJson ", "", 1554913611934L, " x01{source:}".getBytes());
        pkv(map, "row1", "PROPMETA", "ame((workspace))|visallo-confidence", "", 1554914017255L, "x04?xE0x00x00x00x00x00x00".getBytes());
        pkv(map, "row1", "PROPMETA", "ame((workspace))|visallo-confidence", "", 1554913825217L, "x04?xE0x00x00x00x00x00x00".getBytes());
        pkv(map, "row1", "PROPMETA", "ame((workspace))|visallo-confidence", "", 1554913775169L, "x04?xE0x00x00x00x00x00x00".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-confidence", "", 1554913479614L, "x04?xE0x00x00x00x00x00x00".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-modifiedBy", "", 1554914017255L, "x01USER_61e83306f89d40b296e3f6315b01bb67".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-modifiedBy", "", 1554913825217L, "x01USER_61e83306f89d40b296e3f6315b01bb67".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-modifiedBy", "", 1554913775169L, "x01USER_61e83306f89d40b296e3f6315b01bb67".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-modifiedBy", "", 1554913479614L, "x01USER_61e83306f89d40b296e3f6315b01bb67".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-modifiedDate", "", 1554914017255L, "x03x00x00x01jx08x18xFFxC2".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-modifiedDate", "", 1554913825217L, "x03x00x00x01jx08x18xFFxC2".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-modifiedDate", "", 1554913775169L, "x03x00x00x01jx08x18xFFxC2".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-modifiedDate", "", 1554913479614L, "x03x00x00x01jx08x18xFFxC2".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-visibilityJson ", "", 1554914017255L, "source".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-visibilityJson ", "", 1554913825217L, "source".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-visibilityJson ", "", 1554913775169L, "source".getBytes());
        pkv(map, "row1", "PROPMETA", "name((workspace))|visallo-visibilityJson ", "", 1554913479614L, "ein_stuff_row1".getBytes());
        pkv(map, "row1", "V", "", "", 1554913611939L, "".getBytes());

        SortedMapIterator src = new SortedMapIterator(map);
        //Range range = new Range(new Text("row1"), true, new Text("row2"), true);
        Range range = new Range();
        RowDeduplicationIterator iter = new RowDeduplicationIterator();
        Map<String, String> bigBufferOpts = new HashMap<>();
        bigBufferOpts.put(RowDeduplicationIterator.MAX_BUFFER_SIZE_OPT, "300K");
        iter.init(src, bigBufferOpts, new DummyIteratorEnv());
        iter.seek(range, new ArrayList<>(), false);

        TreeMap<Key, Value> tmOut = new TreeMap<>();
        while (iter.hasTop()) {
            tmOut.put(iter.getTopKey(), iter.getTopValue());
            iter.next();
        }
        assertTrue(tmOut.size() == 57);
    }

}
