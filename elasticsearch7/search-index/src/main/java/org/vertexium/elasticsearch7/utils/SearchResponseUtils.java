package org.vertexium.elasticsearch7.utils;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.vertexium.VertexiumException;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

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
}
