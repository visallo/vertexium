package org.vertexium.elasticsearch5;

import org.vertexium.ElementLocation;
import org.vertexium.elasticsearch5.models.Mutation;

import java.util.EnumSet;

public interface IndexSelectionStrategy {
    String[] getIndicesToQuery(Elasticsearch5Graph es);

    String getIndexName(Elasticsearch5Graph es, ElementLocation elementLocation);

    String[] getIndicesToQuery(
        Elasticsearch5Graph es,
        ElasticsearchSearchQueryBase query,
        EnumSet<ElasticsearchDocumentType> elementType
    );

    String getExtendedDataIndexName(
        Elasticsearch5Graph es,
        ElementLocation elementLocation,
        String tableName,
        String rowId
    );

    boolean isIncluded(Elasticsearch5Graph es, String indexName);

    String getMetadataIndexName(Elasticsearch5Graph graph);

    String getMutationIndexName(Elasticsearch5Graph graph, Mutation mutation);

    String getMutationIndexName(Elasticsearch5Graph graph, ElementLocation elementLocation);

    String[] getMutationIndexNames(Elasticsearch5Graph graph);
}
