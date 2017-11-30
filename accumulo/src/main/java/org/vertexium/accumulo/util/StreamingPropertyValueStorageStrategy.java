package org.vertexium.accumulo.util;

import org.vertexium.Property;
import org.vertexium.accumulo.ElementMutationBuilder;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

import java.io.InputStream;
import java.util.List;

public interface StreamingPropertyValueStorageStrategy {
    StreamingPropertyValueRef saveStreamingPropertyValue(
            ElementMutationBuilder elementMutationBuilder,
            String rowKey,
            Property property,
            StreamingPropertyValue streamingPropertyValue
    );

    void close();

    List<InputStream> getInputStreams(List<StreamingPropertyValue> streamingPropertyValues);
}
