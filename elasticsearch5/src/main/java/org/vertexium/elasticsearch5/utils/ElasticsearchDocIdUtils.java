package org.vertexium.elasticsearch5.utils;

import org.elasticsearch.search.SearchHit;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch5.ElasticsearchDocumentType;

public class ElasticsearchDocIdUtils {
    public static Object fromSearchHit(SearchHit searchHit) {
        ElasticsearchDocumentType dt = ElasticsearchDocumentType.fromSearchHit(searchHit);
        if (dt == null) {
            return null;
        }
        switch (dt) {
            case EDGE:
            case VERTEX:
                return searchHit.getId();
            case EDGE_EXTENDED_DATA:
            case VERTEX_EXTENDED_DATA:
                return ElasticsearchExtendedDataIdUtils.fromSearchHit(searchHit);
            default:
                throw new VertexiumException("Unhandled document type: " + dt);
        }
    }
}
