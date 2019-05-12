package org.vertexium.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BatchCollectorTest {
    @Test
    public void testOnlyProcessOnce() {
        List<Item> items = new ArrayList<>();
        for (int i = 0; i < 1001; i++) {
            items.add(new Item(i));
        }

        AtomicBoolean lastBatch = new AtomicBoolean(false);
        AtomicInteger batchCount = new AtomicInteger();
        List<Item> results = items.stream()
            .collect(StreamUtils.batchCollector(100, batch -> {
                if (batch.size() == 1) {
                    lastBatch.set(true);
                } else {
                    assertFalse(lastBatch.get());
                    assertEquals(100, batch.size());
                }
                for (Item item : batch) {
                    assertFalse(item.processed);
                    item.processed = true;
                }
                batchCount.incrementAndGet();
            }));
        assertEquals(0, results.size());
        assertEquals(11, batchCount.get());
    }

    private static class Item {
        public final int i;
        public boolean processed;

        public Item(int i) {
            this.i = i;
        }
    }
}
