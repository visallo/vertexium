package org.vertexium.elasticsearch7;

import org.vertexium.ElementId;
import org.vertexium.ElementLocation;
import org.vertexium.ExtendedDataRowId;
import org.vertexium.PropertyDefinition;

import java.util.EnumSet;

public interface IndexSelectionStrategy {
    String[] getIndicesToQuery(Elasticsearch7SearchIndex es);

    String getIndexName(Elasticsearch7SearchIndex es, ElementId elementId);

    String[] getIndexNames(Elasticsearch7SearchIndex es, PropertyDefinition propertyDefinition);

    boolean isIncluded(Elasticsearch7SearchIndex es, String indexName);

    String[] getManagedIndexNames(Elasticsearch7SearchIndex es);

    String[] getIndicesToQuery(ElasticsearchSearchQueryBase query, EnumSet<ElasticsearchDocumentType> elementType);

    String getExtendedDataIndexName(
        Elasticsearch7SearchIndex es,
        ElementLocation elementLocation,
        String tableName,
        String rowId
    );

    String getExtendedDataIndexName(Elasticsearch7SearchIndex es, ExtendedDataRowId rowId);
}
