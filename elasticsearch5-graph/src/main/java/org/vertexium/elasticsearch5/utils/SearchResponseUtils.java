package org.vertexium.elasticsearch5.utils;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.vertexium.VertexiumException;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.Iterator;

public class SearchResponseUtils {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(SearchResponseUtils.class);

    public static SearchResponse checkForFailures(SearchResponse searchResponse) {
        ShardSearchFailure[] shardFailures = searchResponse.getShardFailures();
        if (shardFailures.length > 0) {
            for (ShardSearchFailure shardFailure : shardFailures) {
                LOGGER.error("search response shard failure", shardFailure.getCause());
            }
            throw new VertexiumException("search response shard failures", shardFailures[0].getCause());
        }
        return searchResponse;
    }

    public static Iterable<SearchHit> scrollToIterable(Client client, SearchResponse results) {
        return () -> new Iterator<SearchHit>() {
            SearchResponse searchResponse = results;
            SearchHit[] hits = results.getHits().getHits();
            int nextHitsIndex = 0;

            @Override
            public boolean hasNext() {
                if (nextHitsIndex < hits.length) {
                    return true;
                }
                searchResponse = client
                    .prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .get();
                nextHitsIndex = 0;
                hits = searchResponse.getHits().getHits();
                return nextHitsIndex < hits.length;
            }

            @Override
            public SearchHit next() {
                SearchHit nextSearchHit = hits[nextHitsIndex];
                nextHitsIndex++;
                return nextSearchHit;
            }
        };
    }
}
