package org.vertexium.query;

@Deprecated
public interface IterableWithTermsResults<T> extends Iterable<T> {
    @Deprecated
    TermsResult getTermsResults(String name);
}
