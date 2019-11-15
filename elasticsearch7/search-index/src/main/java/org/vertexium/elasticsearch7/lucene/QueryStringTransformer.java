package org.vertexium.elasticsearch7.lucene;

import org.vertexium.Authorizations;

public interface QueryStringTransformer {
    String transform(String queryString, Authorizations authorizations);
}
