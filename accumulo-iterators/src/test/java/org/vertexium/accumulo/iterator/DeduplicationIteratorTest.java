package org.vertexium.accumulo.iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.hadoop.io.Text;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeduplicationIteratorTest {
    private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
    private static final Logger log = LoggerFactory.getLogger(DeduplicationIteratorTest.class);

    TreeMap<Key, Value> iteratorOverTestData(DeduplicationIterator it) throws IOException {
        TreeMap<Key, Value> tmOut = new TreeMap<>();
        while (it.hasTop()) {
            tmOut.put(it.getTopKey(), it.getTopValue());
            it.next();
        }
        return tmOut;
    }

    @Test
    public void test1() {
        Text colf = new Text("a");
        Text colq = new Text("b");
        TreeMap<Key, Value> tm = new TreeMap<>();
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 0), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 1), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 2), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 3), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 4), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 5), new Value("red"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 6), new Value("red"));
        assertEquals(7, tm.size());
        try {
            DeduplicationIterator it = new DeduplicationIterator();
            IteratorSetting is = new IteratorSetting(1, DeduplicationIterator.class);
            it.init(new SortedMapIterator(tm), is.getOptions(), null);
            it.seek(new Range(), EMPTY_COL_FAMS, false);

            TreeMap<Key, Value> tmOut = iteratorOverTestData(it);
            int i = 0;
            for (Entry<Key, Value> e : tmOut.entrySet()) {
                if (i == 0) {
                    assertEquals("red", e.getValue().toString());
                    assertEquals(5, e.getKey().getTimestamp());
                }
                if (i == 1) {
                    assertEquals("yellow", e.getValue().toString());
                    assertEquals(1, e.getKey().getTimestamp());
                }
                if (i == 2) {
                    assertEquals("blue", e.getValue().toString());
                    assertEquals(0, e.getKey().getTimestamp());
                }
                System.out.println(e.getKey());
                System.out.println(e.getValue());
                i++;
            }
            assertEquals("size after deduplicating is " + tmOut.size(), 3, tmOut.size());
        } catch (IOException e) {
            fail();
        } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
            fail();
        }
    }

    @Test
    public void test2() {
        Text colf = new Text("a");
        Text colq = new Text("b");
        TreeMap<Key, Value> tm = new TreeMap<>();
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 0), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 1), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 2), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 3), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 4), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 5), new Value("red"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 6), new Value("red"));
        assertEquals(7, tm.size());
        try {
            DeduplicationIterator it = new DeduplicationIterator();
            IteratorSetting is = new IteratorSetting(1, DeduplicationIterator.class);
            it.init(new SortedMapIterator(tm), is.getOptions(), null);
            it.seek(new Range(), EMPTY_COL_FAMS, false);

            TreeMap<Key, Value> tmOut = iteratorOverTestData(it);
            int i = 0;
            for (Entry<Key, Value> e : tmOut.entrySet()) {
                if (i == 0) {
                    assertEquals("red", e.getValue().toString());
                    assertEquals(5, e.getKey().getTimestamp());
                }
                if (i == 1) {
                    assertEquals("yellow", e.getValue().toString());
                    assertEquals(4, e.getKey().getTimestamp());
                }
                if (i == 2) {
                    assertEquals("blue", e.getValue().toString());
                    assertEquals(3, e.getKey().getTimestamp());
                }
                if (i == 3) {
                    assertEquals("yellow", e.getValue().toString());
                    assertEquals(1, e.getKey().getTimestamp());
                }
                if (i == 4) {
                    assertEquals("blue", e.getValue().toString());
                    assertEquals(0, e.getKey().getTimestamp());
                }
                System.out.println(e.getKey());
                System.out.println(e.getValue());
                i++;
            }
            assertEquals("size after deduplicating is " + tmOut.size(), 5, tmOut.size());
        } catch (IOException e) {
            fail();
        } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
            fail();
        }
    }

    @Test
    public void test3() {
        Text colf = new Text("a");
        Text colq = new Text("b");
        TreeMap<Key, Value> tm = new TreeMap<>();
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 0), new Value("blue"));
        assertEquals(1, tm.size());
        try {
            DeduplicationIterator it = new DeduplicationIterator();
            IteratorSetting is = new IteratorSetting(1, DeduplicationIterator.class);
            it.init(new SortedMapIterator(tm), is.getOptions(), null);
            it.seek(new Range(), EMPTY_COL_FAMS, false);

            TreeMap<Key, Value> tmOut = iteratorOverTestData(it);
            for (Entry<Key, Value> e : tmOut.entrySet()) {
                assertEquals("blue", e.getValue().toString());
                assertEquals(0, e.getKey().getTimestamp());
                System.out.println(e.getKey());
                System.out.println(e.getValue());
            }
            assertEquals("size after deduplicating is " + tmOut.size(), 1, tmOut.size());
        } catch (IOException e) {
            fail();
        } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
            fail();
        }
    }

    @Test
    public void test4() {
        Text colf = new Text("a");
        Text colq = new Text("b");
        TreeMap<Key, Value> tm = new TreeMap<>();
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 0), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 1), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 2), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 3), new Value("blue"));
        assertEquals(4, tm.size());
        try {
            DeduplicationIterator it = new DeduplicationIterator();
            IteratorSetting is = new IteratorSetting(1, DeduplicationIterator.class);
            it.init(new SortedMapIterator(tm), is.getOptions(), null);
            it.seek(new Range(), EMPTY_COL_FAMS, false);

            TreeMap<Key, Value> tmOut = iteratorOverTestData(it);
            for (Entry<Key, Value> e : tmOut.entrySet()) {
                assertEquals("blue", e.getValue().toString());
                assertEquals(0, e.getKey().getTimestamp());
                System.out.println(e.getKey());
                System.out.println(e.getValue());
            }
            assertEquals("size after deduplicating is " + tmOut.size(), 1, tmOut.size());
        } catch (IOException e) {
            fail();
        } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
            fail();
        }
    }

    @Test
    public void test5() {
        Text colf = new Text("a");
        Text colq = new Text("b");
        TreeMap<Key, Value> tm = new TreeMap<>();
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 0), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 1), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 2), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 3), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 1)), colf, colq, 4), new Value("red"));
        tm.put(new Key(new Text(String.format("%03d", 1)), colf, colq, 5), new Value("red"));
        tm.put(new Key(new Text(String.format("%03d", 1)), colf, colq, 6), new Value("red"));
        tm.put(new Key(new Text(String.format("%03d", 1)), colf, colq, 7), new Value("red"));
        assertEquals(8, tm.size());
        try {
            DeduplicationIterator it = new DeduplicationIterator();
            IteratorSetting is = new IteratorSetting(1, DeduplicationIterator.class);
            it.init(new SortedMapIterator(tm), is.getOptions(), null);
            it.seek(new Range(), EMPTY_COL_FAMS, false);

            TreeMap<Key, Value> tmOut = iteratorOverTestData(it);
            int i = 0;
            for (Entry<Key, Value> e : tmOut.entrySet()) {
                if (i == 0) {
                    assertEquals("blue", e.getValue().toString());
                    assertEquals(0, e.getKey().getTimestamp());
                }
                if (i == 1) {
                    assertEquals("red", e.getValue().toString());
                    assertEquals(4, e.getKey().getTimestamp());
                }
                System.out.println(e.getKey());
                System.out.println(e.getValue());
                i++;
            }
            assertEquals("size after deduplicating is " + tmOut.size(), 2, tmOut.size());
        } catch (IOException e) {
            fail();
        } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
            fail();
        }
    }

    @Test
    public void test6() {
        Text colf = new Text("a");
        Text colq = new Text("b");
        TreeMap<Key, Value> tm = new TreeMap<>();
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 0), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 1), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 2), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 3), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 1)), colf, colq, 4), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 1)), colf, colq, 5), new Value("red"));
        tm.put(new Key(new Text(String.format("%03d", 1)), colf, colq, 6), new Value("red"));
        tm.put(new Key(new Text(String.format("%03d", 1)), colf, colq, 7), new Value("red"));
        assertEquals(8, tm.size());
        try {
            DeduplicationIterator it = new DeduplicationIterator();
            IteratorSetting is = new IteratorSetting(1, DeduplicationIterator.class);
            it.init(new SortedMapIterator(tm), is.getOptions(), null);
            it.seek(new Range(), EMPTY_COL_FAMS, false);

            TreeMap<Key, Value> tmOut = iteratorOverTestData(it);
            int i = 0;
            for (Entry<Key, Value> e : tmOut.entrySet()) {
                if (i == 0) {
                    assertEquals("blue", e.getValue().toString());
                    assertEquals(3, e.getKey().getTimestamp());
                }
                if (i == 1) {
                    assertEquals("yellow", e.getValue().toString());
                    assertEquals(1, e.getKey().getTimestamp());
                }
                if (i == 2) {
                    assertEquals("blue", e.getValue().toString());
                    assertEquals(0, e.getKey().getTimestamp());
                }
                if (i == 3) {
                    assertEquals("red", e.getValue().toString());
                    assertEquals(5, e.getKey().getTimestamp());
                }
                if (i == 4) {
                    assertEquals("yellow", e.getValue().toString());
                    assertEquals(4, e.getKey().getTimestamp());
                }
                System.out.println(e.getKey());
                System.out.println(e.getValue());
                i++;
            }
            assertEquals("size after deduplicating is " + tmOut.size(), 5, tmOut.size());
        } catch (IOException e) {
            fail();
        } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
            fail();
        }
    }

    @Test
    public void test7() {
        Text colf = new Text("a");
        Text colq = new Text("b");
        TreeMap<Key, Value> tm = new TreeMap<>();
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 0), new Value("blue"));
        try {
            DeduplicationIterator it = new DeduplicationIterator();
            IteratorSetting is = new IteratorSetting(1, DeduplicationIterator.class);
            it.init(new SortedMapIterator(tm), is.getOptions(), null);
            it.seek(new Range(), EMPTY_COL_FAMS, false);

            if (it.hasTop()) {
                assertEquals(0, it.getTopKey().getTimestamp());
                assertEquals("blue", it.getTopValue().toString());
            }

        } catch (IOException e) {
            fail();
        } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
            fail();
        }
    }

    @Test
    public void test8() {
        Text colf = new Text("a");
        Text colq = new Text("b");
        TreeMap<Key, Value> tm = new TreeMap<>();
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 0), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 1), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, 2), new Value("blue"));
        try {
            DeduplicationIterator it = new DeduplicationIterator();
            IteratorSetting is = new IteratorSetting(1, DeduplicationIterator.class);
            it.init(new SortedMapIterator(tm), is.getOptions(), null);
            it.seek(new Range(), EMPTY_COL_FAMS, false);

            if (it.hasTop()) {
                assertEquals(0, it.getTopKey().getTimestamp());
                assertEquals("blue", it.getTopValue().toString());
            }

        } catch (IOException e) {
            fail();
        } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
            fail();
        }
    }

    @Test
    public void test9() {
        Text colf = new Text("a");
        Text colq = new Text("b");
        Text vis1 = new Text("vis1");
        Text vis2 = new Text("vis2");
        TreeMap<Key, Value> tm = new TreeMap<>();
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, vis1, 0), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, vis1, 1), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, vis1, 2), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, vis2, 3), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, vis1, 4), new Value("blue"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, vis1, 5), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, vis1, 6), new Value("yellow"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, vis1, 7), new Value("red"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, vis2, 8), new Value("red"));
        tm.put(new Key(new Text(String.format("%03d", 0)), colf, colq, vis2, 9), new Value("red"));
        assertEquals(10, tm.size());
        try {
            DeduplicationIterator it = new DeduplicationIterator();
            IteratorSetting is = new IteratorSetting(1, DeduplicationIterator.class);
            it.init(new SortedMapIterator(tm), is.getOptions(), null);
            it.seek(new Range(), EMPTY_COL_FAMS, false);

            TreeMap<Key, Value> tmOut = iteratorOverTestData(it);
            int i = 0;
            for (Entry<Key, Value> e : tmOut.entrySet()) {
                if (i == 0) {
                    assertEquals("red", e.getValue().toString());
                    assertEquals(7, e.getKey().getTimestamp());
                }
                if (i == 1) {
                    assertEquals("yellow", e.getValue().toString());
                    assertEquals(5, e.getKey().getTimestamp());
                }
                if (i == 2) {
                    assertEquals("blue", e.getValue().toString());
                    assertEquals(4, e.getKey().getTimestamp());
                }
                if (i == 3) {
                    assertEquals("yellow", e.getValue().toString());
                    assertEquals(1, e.getKey().getTimestamp());
                }
                if (i == 4) {
                    assertEquals("blue", e.getValue().toString());
                    assertEquals(0, e.getKey().getTimestamp());
                }
                if (i == 5) {
                    assertEquals("red", e.getValue().toString());
                    assertEquals(8, e.getKey().getTimestamp());
                }
                if (i == 6) {
                    assertEquals("yellow", e.getValue().toString());
                    assertEquals(3, e.getKey().getTimestamp());
                }
                System.out.println(e.getKey());
                System.out.println(e.getValue());
                i++;
            }
            assertEquals("size after deduplicating is " + tmOut.size(), 7, tmOut.size());
        } catch (IOException e) {
            fail();
        } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
            fail();
        }
    }

}