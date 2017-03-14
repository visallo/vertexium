package org.vertexium.elasticsearch;

import org.vertexium.VertexiumObject;
import org.vertexium.query.QueryParameters;

import java.util.ArrayList;

public class EmptyElasticSearchGraphQueryIterable<T extends VertexiumObject> extends ElasticSearchGraphQueryIterable<T> {
    public EmptyElasticSearchGraphQueryIterable(ElasticSearchSingleDocumentSearchQueryBase query, QueryParameters parameters) {
        super(query, null, parameters, new ArrayList<T>(), false, false, false, 0, 0, null);
    }
}
