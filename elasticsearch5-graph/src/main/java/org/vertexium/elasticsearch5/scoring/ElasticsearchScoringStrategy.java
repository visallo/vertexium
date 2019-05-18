package org.vertexium.elasticsearch5.scoring;

import org.elasticsearch.index.query.QueryBuilder;
import org.vertexium.elasticsearch5.Elasticsearch5Graph;
import org.vertexium.query.QueryParameters;
import org.vertexium.scoring.ScoringStrategy;

public interface ElasticsearchScoringStrategy extends ScoringStrategy {
    QueryBuilder updateElasticsearchQuery(
        Elasticsearch5Graph graph,
        QueryBuilder query,
        QueryParameters queryParameters
    );
}
