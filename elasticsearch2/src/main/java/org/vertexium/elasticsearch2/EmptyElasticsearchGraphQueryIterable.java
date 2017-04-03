package org.vertexium.elasticsearch2;

import org.vertexium.query.QueryParameters;

import java.util.ArrayList;

public class EmptyElasticsearchGraphQueryIterable<T> extends ElasticsearchGraphQueryIterable<T> {
    public EmptyElasticsearchGraphQueryIterable(ElasticsearchSearchQueryBase query, QueryParameters parameters) {
        super(query, null, parameters, new ArrayList<T>(), false, false, false, 0, 0, null);
    }
}
