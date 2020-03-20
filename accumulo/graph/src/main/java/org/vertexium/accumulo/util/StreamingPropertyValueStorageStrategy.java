package org.vertexium.accumulo.util;

import org.vertexium.Property;
import org.vertexium.StreamingPropertyValueChunk;
import org.vertexium.accumulo.ElementMutationBuilder;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Stream;

public interface StreamingPropertyValueStorageStrategy {
    StreamingPropertyValueRef saveStreamingPropertyValue(
        ElementMutationBuilder elementMutationBuilder,
        String rowKey,
        Property property,
        StreamingPropertyValue streamingPropertyValue
    );

    void close();

    List<InputStream> getInputStreams(List<StreamingPropertyValue> streamingPropertyValues);

    default Stream<StreamingPropertyValueChunk> readStreamingPropertyValueChunks(
        Iterable<StreamingPropertyValue> streamingPropertyValues
    ) {
        return StreamingPropertyValue.readChunks(streamingPropertyValues);
    }
}
