package org.vertexium.elasticsearch;

import org.vertexium.Element;
import org.vertexium.query.QueryParameters;

import java.util.ArrayList;

public class EmptyElasticSearchGraphQueryIterable<T extends Element> extends ElasticSearchGraphQueryIterable<T> {
    public EmptyElasticSearchGraphQueryIterable(ElasticSearchQueryBase query, QueryParameters parameters) {
        super(query, null, parameters, new ArrayList<T>(), false, false, false, 0, 0, null);
    }
}
