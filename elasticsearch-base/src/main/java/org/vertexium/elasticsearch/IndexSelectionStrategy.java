package org.vertexium.elasticsearch;

import org.vertexium.Element;
import org.vertexium.PropertyDefinition;

public interface IndexSelectionStrategy {
    String[] getIndicesToQuery(ElasticSearchSearchIndexBase es);

    String getIndexName(ElasticSearchSearchIndexBase es, Element element);

    String[] getIndexNames(ElasticSearchSearchIndexBase es, PropertyDefinition propertyDefinition);

    boolean isIncluded(ElasticSearchSearchIndexBase es, String indexName);

    String[] getManagedIndexNames(ElasticSearchSearchIndexBase es);

    String[] getIndicesToQuery(ElasticSearchQueryBase query, ElasticSearchElementType elementType);
}
