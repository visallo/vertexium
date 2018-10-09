package org.vertexium.elasticsearch5;

import org.elasticsearch.client.Client;

import java.util.HashMap;
import java.util.Map;

public class IndexRefreshTracker {
    private final Map<String, Long> indexToMaxRefreshTime = new HashMap<>();

    public void pushChange(String indexName) {
        synchronized (indexToMaxRefreshTime) {
            indexToMaxRefreshTime.put(indexName, System.currentTimeMillis());
        }
    }

    public void refresh(Client client) {
        long time = System.currentTimeMillis();

        String[] indexNamesNeedingRefresh = getIndexNamesNeedingRefresh(time);
        if (indexNamesNeedingRefresh.length == 0) {
            return;
        }
        client.admin().indices().prepareRefresh(indexNamesNeedingRefresh).execute().actionGet();
        removeRefreshedIndexNames(indexNamesNeedingRefresh, time);
    }

    private String[] getIndexNamesNeedingRefresh(long time) {
        synchronized (indexToMaxRefreshTime) {
            return indexToMaxRefreshTime.entrySet().stream()
                    .filter(e -> e.getValue() <= time)
                    .map(Map.Entry::getKey)
                    .toArray(String[]::new);
        }
    }

    private void removeRefreshedIndexNames(String[] indexNamesNeedingRefresh, long time) {
        synchronized (indexToMaxRefreshTime) {
            for (String indexName : indexNamesNeedingRefresh) {
                if (indexToMaxRefreshTime.getOrDefault(indexName, Long.MAX_VALUE) <= time) {
                    indexToMaxRefreshTime.remove(indexName);
                }
            }
        }
    }
}
