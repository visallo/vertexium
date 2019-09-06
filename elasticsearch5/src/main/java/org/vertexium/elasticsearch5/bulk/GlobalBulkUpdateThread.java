package org.vertexium.elasticsearch5.bulk;

import org.vertexium.VertexiumException;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class GlobalBulkUpdateThread {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(BulkUpdateService.class);
    private final Thread watchThread;
    private boolean run;

    public GlobalBulkUpdateThread(long autoFlushTimeMs) {
        watchThread = new Thread(() -> {
            while (run) {
                try {
                    cleanupOldThreads();
                } catch (Exception ex) {
                    LOGGER.error("failed to clean up old threads", ex);
                }

                try {
                    while (flushSingleBatch(autoFlushTimeMs)) {
                        // keep flushing until no more items to flush
                    }
                } catch (Exception ex) {
                    LOGGER.error("failed to watch for bulk updates (sleeping then trying again)", ex);
                }

                try {
                    Thread.sleep(autoFlushTimeMs);
                } catch (InterruptedException e) {
                    LOGGER.error("failed to sleep", e);
                    break;
                }
            }
        });
        watchThread.setDaemon(true);
        watchThread.setName("bulk-update-watch");
    }

    private void cleanupOldThreads() {
        Set<Long> activeThreadIds = getActiveThreadIds();
        for (Map.Entry<Long, BulkUpdateQueue> entry : getBulkUpdateQueues()) {
            if (!activeThreadIds.contains(entry.getKey())) {
                entry.getValue().flush();
                removeBulkUpdateQueue(entry.getKey());
            }
        }
    }

    protected abstract void removeBulkUpdateQueue(long threadId);

    private Set<Long> getActiveThreadIds() {
        return Thread.getAllStackTraces().keySet().stream()
            .map(Thread::getId)
            .collect(Collectors.toSet());
    }

    protected abstract Set<Map.Entry<Long, BulkUpdateQueue>> getBulkUpdateQueues();

    private boolean flushSingleBatch(long autoFlushTimeMs) {
        BulkUpdateQueue bulkUpdateQueue = getBulkUpdateQueueWithOldestActiveItem();
        if (bulkUpdateQueue == null) {
            return false;
        }

        Long oldestEnqueueTime = bulkUpdateQueue.getOldestTodoItemTime();
        if (oldestEnqueueTime == null) {
            return false;
        }

        if (oldestEnqueueTime + autoFlushTimeMs > System.currentTimeMillis()) {
            return false;
        }

        bulkUpdateQueue.flushSingleBatch(); // ignore the result the items are queued which is good enough
        return true;
    }

    protected abstract BulkUpdateQueue getBulkUpdateQueueWithOldestActiveItem();

    public void start() {
        run = true;
        watchThread.start();
    }

    public void shutdown() {
        run = false;
        try {
            watchThread.join();
        } catch (InterruptedException ex) {
            throw new VertexiumException("Could not stop thread", ex);
        }
    }
}
