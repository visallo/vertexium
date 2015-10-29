package org.vertexium.elasticsearch;

import org.vertexium.Element;
import org.vertexium.PropertyDefinition;

public interface IndexSelectionStrategy {
    String[] getIndicesToQuery(ElasticsearchSingleDocumentSearchIndex es);

    String getIndexName(ElasticsearchSingleDocumentSearchIndex es, Element element);

    String[] getIndexNames(ElasticsearchSingleDocumentSearchIndex es, PropertyDefinition propertyDefinition);

    boolean isIncluded(ElasticsearchSingleDocumentSearchIndex es, String indexName);

    String[] getManagedIndexNames(ElasticsearchSingleDocumentSearchIndex es);

    String[] getIndicesToQuery(ElasticSearchSingleDocumentSearchQueryBase query, ElasticSearchElementType elementType);
}
