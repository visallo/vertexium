package org.vertexium.elasticsearch5;

import com.google.common.collect.Lists;
import org.elasticsearch.client.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class IndexRefreshTracker {
    private final ReadWriteLock indexToMaxRefreshTimeLock = new ReentrantReadWriteLock();
    private final Map<String, Long> indexToMaxRefreshTime = new HashMap<>();
    private final Lock readLock = indexToMaxRefreshTimeLock.readLock();
    private final Lock writeLock = indexToMaxRefreshTimeLock.writeLock();

    public void pushChange(String indexName) {
        writeLock.lock();
        try {
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
        client.admin().indices().prepareRefresh(indexNamesNeedingRefresh.toArray(new String[0])).execute().actionGet();
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
