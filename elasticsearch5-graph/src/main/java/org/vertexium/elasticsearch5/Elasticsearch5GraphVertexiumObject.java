package org.vertexium.elasticsearch5;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.vertexium.*;
import org.vertexium.util.StreamUtils;

class Elasticsearch5GraphVertexiumObject {
    static VertexiumObject createVertexiumObject(Elasticsearch5Graph graph, SearchHit hit, FetchHints fetchHints, User user) {
        ElasticsearchDocumentType elementType = ElasticsearchDocumentType.parse(hit.getField(FieldNames.ELEMENT_TYPE).getValue());
        switch (elementType) {
            case VERTEX:
                return new Elasticsearch5GraphVertex(graph, hit, fetchHints, user);
            case EDGE:
                return new Elasticsearch5GraphEdge(graph, hit, fetchHints, user);
            case VERTEX_EXTENDED_DATA:
            case EDGE_EXTENDED_DATA:
                return new Elasticsearch5GraphExtendedDataRow(graph, hit, fetchHints, user);
            default:
                throw new VertexiumException("not implemented");
        }
    }

    static ImmutableSet<Visibility> readAdditionalVisibilitiesFromSearchHit(SearchHit hit) {
        SearchHitField field = hit.getField(FieldNames.ADDITIONAL_VISIBILITY);
        if (field == null) {
            return ImmutableSet.of();
        }
        return field.getValues().stream()
            .map(s -> new Visibility((String) s))
            .collect(StreamUtils.toImmutableSet());
    }
}
