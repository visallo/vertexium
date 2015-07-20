package org.vertexium.elasticsearch;

import org.vertexium.Element;
import org.vertexium.PropertyDefinition;

public interface IndexSelectionStrategy {
    String[] getIndicesToQuery();

    String getIndexName(Element element);

    String[] getIndexNames(PropertyDefinition propertyDefinition);
}
