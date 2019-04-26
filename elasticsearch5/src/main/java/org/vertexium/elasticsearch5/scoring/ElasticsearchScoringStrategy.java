package org.vertexium.elasticsearch5.scoring;

import org.elasticsearch.index.query.QueryBuilder;
import org.vertexium.Graph;
import org.vertexium.elasticsearch5.Elasticsearch5SearchIndex;
import org.vertexium.query.QueryParameters;
import org.vertexium.scoring.ScoringStrategy;

public interface ElasticsearchScoringStrategy extends ScoringStrategy {
    QueryBuilder updateElasticsearchQuery(
        Graph graph,
        Elasticsearch5SearchIndex searchIndex,
        QueryBuilder query,
        QueryParameters queryParameters
    );
}
