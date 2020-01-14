package org.vertexium.elasticsearch7.bulk;

import org.vertexium.VertexiumException;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class OutstandingItemsList {
    private final LinkedList<Item> outstandingItems = new LinkedList<>();
    private final Set<String> inflightItems = new HashSet<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition itemsChanged = lock.newCondition();

    public void add(Item bulkItem) {
        lock.lock();
        try {
            outstandingItems.add(bulkItem);
            itemsChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void remove(Item item) {
        lock.lock();
        try {
            outstandingItems.remove(item);
            itemsChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void removeAll(Collection<? extends Item> items) {
        lock.lock();
        try {
            outstandingItems.removeAll(items);
            itemsChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        return outstandingItems.size();
    }

    public void markItemsAsNotInflight(List<BulkItem<?>> bulkItems) {
        lock.lock();
        try {
            for (BulkItem<?> bulkItem : bulkItems) {
                inflightItems.remove(getInflightKey(bulkItem));
            }
            itemsChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private String getInflightKey(BulkItem<?> bulkItem) {
        return String.format("%s:%s:%s", bulkItem.getIndexName(), bulkItem.getType(), bulkItem.getDocumentId());
    }

    public void waitForItemToNotBeInflightAndMarkThemAsInflight(List<BulkItem<?>> bulkItems) {
        lock.lock();
        try {
            while (true) {
                boolean hasInflightItems = false;
                for (BulkItem<?> bulkItem : bulkItems) {
                    if (inflightItems.contains(getInflightKey(bulkItem))) {
                        hasInflightItems = true;
                        break;
                    }
                }
                if (hasInflightItems) {
                    itemsChanged.await(1, TimeUnit.SECONDS);
                } else {
                    for (BulkItem<?> bulkItem : bulkItems) {
                        inflightItems.add(getInflightKey(bulkItem));
                    }
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new VertexiumException("Failed to wait for items to not be inflight");
        } finally {
            lock.unlock();
        }
    }

    public List<Item> getCopyOfItems() {
        lock.lock();
        try {
            return new ArrayList<>(outstandingItems);
        } finally {
            lock.unlock();
        }
    }
}
