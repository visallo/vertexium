package org.vertexium.elasticsearch5;

import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;

public interface StreamingPropertyValueService {
    org.vertexium.elasticsearch5.models.StreamingPropertyValueRef save(StreamingPropertyValue value);

    StreamingPropertyValue read(StreamingPropertyValueRef<Elasticsearch5Graph> streamingPropertyValueRef, long timestamp);

    StreamingPropertyValue fromProtobuf(org.vertexium.elasticsearch5.models.StreamingPropertyValueRef storedValue);
}
