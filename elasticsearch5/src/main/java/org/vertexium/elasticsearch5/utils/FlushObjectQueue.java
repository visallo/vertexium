package org.vertexium.elasticsearch5.utils;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.vertexium.ElementLocation;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.Elasticsearch5SearchIndex;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FlushObjectQueue {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(FlushObjectQueue.class);
    private static final int MAX_RETRIES = 10;
    private final Elasticsearch5SearchIndex searchIndex;
    private final Queue<FlushObject> queue = new ConcurrentLinkedQueue<>();

    public FlushObjectQueue(Elasticsearch5SearchIndex searchIndex) {
        this.searchIndex = searchIndex;
    }

    public void flush() {
        int sleep = 0;
        int itemsToFlush = queue.size();
        while (itemsToFlush > 0) {
            FlushObject flushObject = removeNext();
            if (flushObject == null) {
                break;
            }
            try {
                flushObject.getFuture().get(30, TimeUnit.MINUTES);
                itemsToFlush--;
                sleep = 0;
            } catch (Exception ex) {
                if (isDocumentMissingException(ex)) {
                    try {
                        searchIndex.handleDocumentMissingException(flushObject, ex);
                    } catch (Exception e) {
                        LOGGER.error("Failed to handle document missing exception", e);
                        throw new VertexiumException(ex);
                    }
                    return;
                }

                sleep += 10;
                String message = String.format("Could not write %s", flushObject);
                if (flushObject.retryCount >= MAX_RETRIES) {
                    throw new VertexiumException(message, ex);
                }
                String logMessage = String.format("%s: %s (retrying: %d/%d)", message, ex.getMessage(), flushObject.retryCount + 1, MAX_RETRIES);
                if (flushObject.retryCount > 0) { // don't log warn the first time
                    LOGGER.warn("%s", logMessage);
                } else {
                    LOGGER.debug("%s", logMessage);
                }

                requeueFlushObject(flushObject, sleep);
                itemsToFlush = queue.size();
            }
        }
    }

    private FlushObject removeNext() {
        synchronized (queue) {
            if (queue.isEmpty()) {
                return null;
            }
            return queue.remove();
        }
    }

    private boolean isDocumentMissingException(Throwable ex) {
        if (ex instanceof DocumentMissingException) {
            return true;
        }
        if (ex.getCause() != null) {
            return isDocumentMissingException(ex.getCause());
        }
        return false;
    }

    private void requeueFlushObject(FlushObject flushObject, int additionalTimeToSleep) {
        try {
            Thread.sleep(Math.max(0, flushObject.getNextRetryTime() - System.currentTimeMillis()));
        } catch (InterruptedException ex) {
            throw new VertexiumException("failed to sleep", ex);
        }
        long timeToWait = Math.min(
            ((flushObject.getRetryCount() + 1) * 10) + additionalTimeToSleep,
            1 * 60 * 1000
        );
        long nextRetryTime = System.currentTimeMillis() + timeToWait;
        queue.add(new FlushObject(
            flushObject.getElementLocation(),
            flushObject.getExtendedDataTableName(),
            flushObject.getExtendedDataRowId(),
            flushObject.getActionRequestBuilder(),
            flushObject.getActionRequestBuilder().execute(),
            flushObject.getRetryCount() + 1,
            nextRetryTime
        ));
    }

    public void add(
        ElementLocation elementLocation,
        String extendedDataTableName,
        String rowId,
        UpdateRequestBuilder updateRequestBuilder,
        Future future
    ) {
        queue.add(new FlushObject(elementLocation, extendedDataTableName, rowId, updateRequestBuilder, future));
    }

    public boolean containsElementId(String elementId) {
        for (FlushObject flushObject : queue) {
            if (flushObject.getElementLocation().getId().equals(elementId)) {
                return true;
            }
        }
        return false;
    }

    public static class FlushObject {
        private final ElementLocation elementLocation;
        private final String extendedDataTableName;
        private final String extendedDataRowId;
        private final ActionRequestBuilder actionRequestBuilder;
        private final Future future;
        private final int retryCount;
        private final long nextRetryTime;

        FlushObject(
            ElementLocation elementLocation,
            String extendedDataTableName,
            String extendedDataRowId,
            UpdateRequestBuilder updateRequestBuilder,
            Future future
        ) {
            this(elementLocation, extendedDataTableName, extendedDataRowId, updateRequestBuilder, future, 0, 0);
        }

        FlushObject(
            ElementLocation elementLocation,
            String extendedDataTableName,
            String extendedDataRowId,
            ActionRequestBuilder actionRequestBuilder,
            Future future,
            int retryCount,
            long nextRetryTime
        ) {
            this.elementLocation = elementLocation;
            this.extendedDataTableName = extendedDataTableName;
            this.extendedDataRowId = extendedDataRowId;
            this.actionRequestBuilder = actionRequestBuilder;
            this.future = future;
            this.retryCount = retryCount;
            this.nextRetryTime = nextRetryTime;
        }

        @Override
        public String toString() {
            String elementId = getElementLocation().getId();
            if (extendedDataRowId == null) {
                return String.format("Element \"%s\"", elementId);
            } else {
                return String.format("Extended data row \"%s\":\"%s\":\"%s\"", elementId, extendedDataTableName, extendedDataRowId);
            }
        }

        public ElementLocation getElementLocation() {
            return elementLocation;
        }

        public String getExtendedDataTableName() {
            return extendedDataTableName;
        }

        public String getExtendedDataRowId() {
            return extendedDataRowId;
        }

        public ActionRequestBuilder getActionRequestBuilder() {
            return actionRequestBuilder;
        }

        public Future getFuture() {
            return future;
        }

        public int getRetryCount() {
            return retryCount;
        }

        public long getNextRetryTime() {
            return nextRetryTime;
        }
    }
}