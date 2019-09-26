package org.vertexium.elasticsearch5;

import com.google.common.collect.Lists;
import org.elasticsearch.client.Client;
import org.vertexium.metric.Counter;
import org.vertexium.metric.Timer;
import org.vertexium.metric.VertexiumMetricRegistry;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class IndexRefreshTracker {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(IndexRefreshTracker.class);
    private final ReadWriteLock indexToMaxRefreshTimeLock = new ReentrantReadWriteLock();
    private final Map<String, Long> indexToMaxRefreshTime = new HashMap<>();
    private final Lock readLock = indexToMaxRefreshTimeLock.readLock();
    private final Lock writeLock = indexToMaxRefreshTimeLock.writeLock();
    private final Counter pushCounter;
    private final Timer refreshTimer;

    public IndexRefreshTracker(VertexiumMetricRegistry metricRegistry) {
        this.pushCounter = metricRegistry.getCounter(IndexRefreshTracker.class, "push", "counter");
        this.refreshTimer = metricRegistry.getTimer(IndexRefreshTracker.class, "refresh", "timer");
    }

    public void pushChange(String indexName) {
        pushCounter.increment();
        writeLock.lock();
        try {
            LOGGER.debug("index added for refresh: %s", indexName);
            indexToMaxRefreshTime.put(indexName, getTime());
        } finally {
            writeLock.unlock();
        }
    }

    public void refresh(Client client) {
        long time = getTime();

        Set<String> indexNamesNeedingRefresh = getIndexNamesNeedingRefresh(time);
        if (indexNamesNeedingRefresh.size() > 0) {
            refresh(client, indexNamesNeedingRefresh);
            removeRefreshedIndexNames(indexNamesNeedingRefresh, time);
        }
    }

    protected long getTime() {
        return System.currentTimeMillis();
    }

    public void refresh(Client client, String... indexNames) {
        long time = getTime();

        Set<String> indexNamesNeedingRefresh = getIndexNamesNeedingRefresh(time);
        indexNamesNeedingRefresh.retainAll(Lists.newArrayList(indexNames));
        if (indexNamesNeedingRefresh.size() > 0) {
            refresh(client, indexNamesNeedingRefresh);
            removeRefreshedIndexNames(indexNamesNeedingRefresh, time);
        }
    }

    protected void refresh(Client client, Set<String> indexNamesNeedingRefresh) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("refreshing: %s", String.join(", ", indexNamesNeedingRefresh));
        }
        refreshTimer.time(() -> {
            client.admin().indices().prepareRefresh(indexNamesNeedingRefresh.toArray(new String[0])).execute().actionGet();
        });
    }

    private Set<String> getIndexNamesNeedingRefresh(long time) {
        readLock.lock();
        try {
            return indexToMaxRefreshTime.entrySet().stream()
                .filter(e -> e.getValue() <= time)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        } finally {
            readLock.unlock();
        }
    }

    private void removeRefreshedIndexNames(Set<String> indexNamesNeedingRefresh, long time) {
        writeLock.lock();
        try {
            for (String indexName : indexNamesNeedingRefresh) {
                if (indexToMaxRefreshTime.getOrDefault(indexName, Long.MAX_VALUE) <= time) {
                    indexToMaxRefreshTime.remove(indexName);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }
}
