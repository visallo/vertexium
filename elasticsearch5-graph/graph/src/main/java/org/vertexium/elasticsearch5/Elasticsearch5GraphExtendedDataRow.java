package org.vertexium.elasticsearch5;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.search.SearchHit;
import org.vertexium.*;
import org.vertexium.elasticsearch5.models.LazyProperties;

public class Elasticsearch5GraphExtendedDataRow extends ExtendedDataRowBase implements ExtendedDataRow {
    private final ExtendedDataRowId id;
    private final ImmutableSet<Visibility> additionalVisibilities;
    private final LazyProperties properties;

    Elasticsearch5GraphExtendedDataRow(Elasticsearch5Graph graph, SearchHit hit, FetchHints fetchHints, User user) {
        super(graph, fetchHints, user);
        ElasticsearchDocumentType docType = ElasticsearchDocumentType.fromSearchHit(hit);
        ElementType elementType;
        switch (docType) {
            case VERTEX_EXTENDED_DATA:
                elementType = ElementType.VERTEX;
                break;
            case EDGE_EXTENDED_DATA:
                elementType = ElementType.EDGE;
                break;
            default:
                throw new VertexiumException("Invalid doc type: " + docType);
        }
        String elementId = hit.getField(FieldNames.ELEMENT_ID).getValue();
        String tableName = hit.getField(FieldNames.EXTENDED_DATA_TABLE_NAME).getValue();
        String rowId = hit.getField(FieldNames.EXTENDED_DATA_TABLE_ROW_ID).getValue();
        this.id = new ExtendedDataRowId(
            elementType,
            elementId,
            tableName,
            rowId
        );
        this.additionalVisibilities = Elasticsearch5GraphVertexiumObject.readAdditionalVisibilitiesFromSearchHit(hit);
        this.properties = new LazyProperties(
            hit.getField(FieldNames.PROPERTIES_DATA),
            fetchHints,
            graph.getStreamingPropertyValueService(),
            graph.getScriptService(),
            user);
    }

    @Override
    public ExtendedDataRowId getId() {
        return id;
    }

    @Override
    public Iterable<Property> getProperties() {
        return properties;
    }

    @Override
    public ImmutableSet<Visibility> getAdditionalVisibilities() {
        return additionalVisibilities;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof ExtendedDataRow) {
            return getId().compareTo(((ExtendedDataRow) o).getId());
        }
        throw new ClassCastException("o must be an " + ExtendedDataRow.class.getName());
    }
}
