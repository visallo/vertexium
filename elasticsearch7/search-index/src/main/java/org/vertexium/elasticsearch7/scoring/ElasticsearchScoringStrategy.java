package org.vertexium.elasticsearch7.scoring;

import org.elasticsearch.index.query.QueryBuilder;
import org.vertexium.Graph;
import org.vertexium.elasticsearch7.Elasticsearch7SearchIndex;
import org.vertexium.query.QueryParameters;
import org.vertexium.scoring.ScoringStrategy;

public interface ElasticsearchScoringStrategy extends ScoringStrategy {
    QueryBuilder updateElasticsearchQuery(
        Graph graph,
        Elasticsearch7SearchIndex searchIndex,
        QueryBuilder query,
        QueryParameters queryParameters
    );
}
