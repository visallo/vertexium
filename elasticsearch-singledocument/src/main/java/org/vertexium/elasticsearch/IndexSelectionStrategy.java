package org.vertexium.elasticsearch;

import org.vertexium.Element;
import org.vertexium.ExtendedDataRowId;
import org.vertexium.PropertyDefinition;

import java.util.EnumSet;

public interface IndexSelectionStrategy {
    String[] getIndicesToQuery(ElasticsearchSingleDocumentSearchIndex es);

    String getIndexName(ElasticsearchSingleDocumentSearchIndex es, Element element);

    String[] getIndexNames(ElasticsearchSingleDocumentSearchIndex es, PropertyDefinition propertyDefinition);

    boolean isIncluded(ElasticsearchSingleDocumentSearchIndex es, String indexName);

    String[] getManagedIndexNames(ElasticsearchSingleDocumentSearchIndex es);

    String[] getIndicesToQuery(ElasticSearchSingleDocumentSearchQueryBase query, EnumSet<ElasticsearchDocumentType> elementType);

    String getExtendedDataIndexName(ElasticsearchSingleDocumentSearchIndex es, Element element, String tableName, String rowId);

    String getExtendedDataIndexName(ElasticsearchSingleDocumentSearchIndex es, ExtendedDataRowId rowId);
}
