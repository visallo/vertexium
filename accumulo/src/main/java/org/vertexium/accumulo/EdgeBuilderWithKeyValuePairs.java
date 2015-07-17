package org.vertexium.accumulo;

public interface EdgeBuilderWithKeyValuePairs {
    Iterable<KeyValuePair> getEdgeTableKeyValuePairs();

    Iterable<KeyValuePair> getVertexTableKeyValuePairs();
}
