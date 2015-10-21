package org.vertexium.query;

@Deprecated
public interface IterableWithGeohashResults<T> extends Iterable<T> {
    @Deprecated
    GeohashResult getGeohashResults(String name);
}
