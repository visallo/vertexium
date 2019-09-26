package org.vertexium.elasticsearch5.bulk;

import org.vertexium.util.VertexiumReentrantReadWriteLock;

import java.util.LinkedList;
import java.util.List;

public class BulkItemList {
    private final VertexiumReentrantReadWriteLock lock = new VertexiumReentrantReadWriteLock();
    private final LinkedList<BulkItem> items = new LinkedList<>();

    public void add(BulkItem bulkItem) {
        lock.executeInWriteLock(() -> {
            bulkItem.updateCreatedOrLastTriedTime();
            items.add(bulkItem);
        });
    }

    public void remove(BulkItem bulkItem) {
        lock.executeInWriteLock(() -> {
            items.remove(bulkItem);
        });
    }

    public void removeAll(List<BulkItem> batch) {
        lock.executeInWriteLock(() -> {
            for (BulkItem bulkItem : batch) {
                items.remove(bulkItem);
            }
        });
    }

    public void addAll(List<BulkItem> batch) {
        lock.executeInWriteLock(() -> {
            for (BulkItem bulkItem : batch) {
                bulkItem.updateCreatedOrLastTriedTime();
                items.add(bulkItem);
            }
        });
    }

    public BulkItem dequeue() {
        return lock.executeInWriteLock(() -> {
            if (items.size() == 0) {
                return null;
            }
            return items.removeFirst();
        });
    }

    public BulkItem peek() {
        return lock.executeInReadLock(items::peek);
    }

    public boolean containsElementId(String elementId) {
        return lock.executeInReadLock(() -> {
            for (BulkItem item : items) {
                if (item.getElementId().getId().equals(elementId)) {
                    return true;
                }
            }
            return false;
        });
    }

    public Long getOldestItemTime() {
        return lock.executeInReadLock(() -> {
            Long oldestTime = null;
            for (BulkItem item : items) {
                if (oldestTime == null || item.getCreatedOrLastTriedTime() < oldestTime) {
                    oldestTime = item.getCreatedOrLastTriedTime();
                }
            }
            return oldestTime;
        });
    }

    public long size(long beforeTime) {
        return lock.executeInReadLock(() -> items.stream()
            .filter(item -> item.getCreatedTime() <= beforeTime)
            .count());
    }
}
