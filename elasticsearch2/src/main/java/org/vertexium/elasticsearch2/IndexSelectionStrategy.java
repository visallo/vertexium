package org.vertexium.elasticsearch2;

import org.vertexium.Element;
import org.vertexium.ExtendedDataRowId;
import org.vertexium.PropertyDefinition;

import java.util.EnumSet;

public interface IndexSelectionStrategy {
    String[] getIndicesToQuery(Elasticsearch2SearchIndex es);

    String getIndexName(Elasticsearch2SearchIndex es, Element element);

    String[] getIndexNames(Elasticsearch2SearchIndex es, PropertyDefinition propertyDefinition);

    boolean isIncluded(Elasticsearch2SearchIndex es, String indexName);

    String[] getManagedIndexNames(Elasticsearch2SearchIndex es);

    String[] getIndicesToQuery(ElasticsearchSearchQueryBase query, EnumSet<ElasticsearchDocumentType> elementType);

    String getExtendedDataIndexName(Elasticsearch2SearchIndex es, Element element, String tableName, String rowId);

    String getExtendedDataIndexName(Elasticsearch2SearchIndex es, ExtendedDataRowId rowId);
}
