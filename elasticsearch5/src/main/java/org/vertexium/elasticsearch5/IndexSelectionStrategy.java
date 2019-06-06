package org.vertexium.elasticsearch5;

import org.vertexium.ElementLocation;
import org.vertexium.ExtendedDataRowId;
import org.vertexium.PropertyDefinition;

import java.util.EnumSet;

public interface IndexSelectionStrategy {
    String[] getIndicesToQuery(Elasticsearch5SearchIndex es);

    String getIndexName(Elasticsearch5SearchIndex es, ElementLocation elementLocation);

    String[] getIndexNames(Elasticsearch5SearchIndex es, PropertyDefinition propertyDefinition);

    boolean isIncluded(Elasticsearch5SearchIndex es, String indexName);

    String[] getManagedIndexNames(Elasticsearch5SearchIndex es);

    String[] getIndicesToQuery(ElasticsearchSearchQueryBase query, EnumSet<ElasticsearchDocumentType> elementType);

    String getExtendedDataIndexName(
        Elasticsearch5SearchIndex es,
        ElementLocation elementLocation,
        String tableName,
        String rowId
    );

    String getExtendedDataIndexName(Elasticsearch5SearchIndex es, ExtendedDataRowId rowId);
}
