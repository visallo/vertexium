package org.vertexium.elasticsearch5;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.vertexium.Authorizations;
import org.vertexium.FetchHints;
import org.vertexium.Graph;
import org.vertexium.query.GraphQuery;

import java.util.EnumSet;
import java.util.List;

public class ElasticsearchSearchExtendedDataQuery extends ElasticsearchSearchQueryBase implements GraphQuery {
    private final String elementId;
    private final String tableName;

    public ElasticsearchSearchExtendedDataQuery(
        Client client,
        Graph graph,
        IndexService indexService,
        PropertyNameService propertyNameService,
        PropertyNameVisibilitiesStore propertyNameVisibilitiesStore,
        String queryString,
        String elementId,
        String tableName,
        Options options,
        Authorizations authorizations
    ) {
        super(client, graph, indexService, propertyNameService, propertyNameVisibilitiesStore, queryString, options, authorizations);
        this.elementId = elementId;
        this.tableName = tableName;
    }

    @Override
    protected List<QueryBuilder> getFilters(EnumSet<ElasticsearchDocumentType> elementTypes, FetchHints fetchHints) {
        List<QueryBuilder> filters = super.getFilters(elementTypes, fetchHints);
        filters.add(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termsQuery(
                    Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME,
                    ElasticsearchDocumentType.VERTEX_EXTENDED_DATA.getKey(),
                    ElasticsearchDocumentType.EDGE_EXTENDED_DATA.getKey()
                ))
                .must(QueryBuilders.termQuery(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME, elementId))
                .must(QueryBuilders.termQuery(Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME, tableName))
        );
        return filters;
    }

    @Override
    public String toString() {
        return super.toString() +
            ", elementId=" + elementId +
            ", tableName=" + tableName;
    }
}
