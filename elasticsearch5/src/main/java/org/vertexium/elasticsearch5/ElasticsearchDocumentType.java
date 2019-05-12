package org.vertexium.elasticsearch5;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.vertexium.Element;
import org.vertexium.ElementType;
import org.vertexium.VertexiumException;
import org.vertexium.VertexiumObjectType;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public enum ElasticsearchDocumentType {
    VERTEX("vertex"),
    EDGE("edge"),
    VERTEX_EXTENDED_DATA("vertexextdata"),
    EDGE_EXTENDED_DATA("edgeextdata");

    public static final EnumSet<ElasticsearchDocumentType> ELEMENTS = EnumSet.of(VERTEX, EDGE);
    private final String key;

    ElasticsearchDocumentType(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public static ElasticsearchDocumentType parse(String s) {
        if (s.equals(VERTEX.getKey())) {
            return VERTEX;
        } else if (s.equals(EDGE.getKey())) {
            return EDGE;
        } else if (s.equals(VERTEX_EXTENDED_DATA.getKey())) {
            return VERTEX_EXTENDED_DATA;
        } else if (s.equals(EDGE_EXTENDED_DATA.getKey())) {
            return EDGE_EXTENDED_DATA;
        }
        throw new VertexiumException("Could not parse element type: " + s);
    }

    public ElementType toElementType() {
        switch (this) {
            case VERTEX:
            case VERTEX_EXTENDED_DATA:
                return ElementType.VERTEX;
            case EDGE:
            case EDGE_EXTENDED_DATA:
                return ElementType.EDGE;
        }
        throw new VertexiumException("Unhandled type: " + this);
    }

    public static EnumSet<ElasticsearchDocumentType> fromVertexiumObjectTypes(EnumSet<VertexiumObjectType> objectTypes) {
        List<ElasticsearchDocumentType> enums = new ArrayList<>();
        for (VertexiumObjectType objectType : objectTypes) {
            switch (objectType) {
                case VERTEX:
                    enums.add(VERTEX);
                    break;
                case EDGE:
                    enums.add(EDGE);
                    break;
                case EXTENDED_DATA:
                    enums.add(VERTEX_EXTENDED_DATA);
                    enums.add(EDGE_EXTENDED_DATA);
                    break;
                default:
                    throw new VertexiumException("Unhandled Vertexium object type: " + objectType);
            }
        }
        return EnumSet.copyOf(enums);
    }

    public static ElasticsearchDocumentType fromSearchHit(SearchHit searchHit) {
        SearchHitField elementType = searchHit.getFields().get(Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME);
        if (elementType == null) {
            return null;
        }
        return ElasticsearchDocumentType.parse(elementType.getValue().toString());
    }

    public static <T extends Element> ElasticsearchDocumentType getExtendedDataDocumentTypeFromElement(
        ElementType elementType
    ) {
        switch (elementType) {
            case VERTEX:
                return ElasticsearchDocumentType.VERTEX_EXTENDED_DATA;
            case EDGE:
                return ElasticsearchDocumentType.EDGE_EXTENDED_DATA;
            default:
                throw new VertexiumException("Unhandled element type: " + elementType);
        }
    }

    public static ElasticsearchDocumentType getExtendedDataDocumentTypeFromElementType(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return VERTEX_EXTENDED_DATA;
            case EDGE:
                return EDGE_EXTENDED_DATA;
            default:
                throw new VertexiumException("Unhandled element type: " + elementType);
        }
    }
}
