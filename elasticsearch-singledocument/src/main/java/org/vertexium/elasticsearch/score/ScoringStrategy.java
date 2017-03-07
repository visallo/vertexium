package org.vertexium.elasticsearch.score;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.vertexium.*;
import org.vertexium.elasticsearch.ElasticsearchSingleDocumentSearchIndex;
import org.vertexium.elasticsearch.IndexInfo;
import org.vertexium.mutation.ExtendedDataMutation;
import org.vertexium.search.SearchIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class ScoringStrategy {
    private final GraphConfiguration graphConfiguration;

    protected ScoringStrategy(GraphConfiguration graphConfiguration) {
        this.graphConfiguration = graphConfiguration;
    }

    protected GraphConfiguration getGraphConfiguration() {
        return graphConfiguration;
    }

    public abstract void addElement(SearchIndex searchIndex, Graph graph, Element element, Authorizations authorizations);

    public abstract boolean addFieldsToVertexDocument(SearchIndex searchIndex, XContentBuilder jsonBuilder, Vertex vertex, GetResponse existingDocument, Authorizations authorizations) throws IOException;

    public abstract boolean addFieldsToEdgeDocument(SearchIndex searchIndex, XContentBuilder jsonBuilder, Edge edge, GetResponse existingDocument, Authorizations authorizations) throws IOException;

    public abstract int addElement(ElasticsearchSingleDocumentSearchIndex searchIndex, Graph graph, BulkRequest bulkRequest, IndexInfo indexInfo, Element element, Authorizations authorizations);

    public abstract void addFieldsToElementType(XContentBuilder builder) throws IOException;

    public abstract QueryBuilder updateQuery(QueryBuilder query);

    public List<String> getFieldNames() {
        return new ArrayList<>();
    }

    public void addElementExtendedData(ElasticsearchSingleDocumentSearchIndex elasticsearchSingleDocumentSearchIndex, Graph graph, Element element, String tableName, String rowId, List<ExtendedDataMutation> columns, Authorizations authorizations) {

    }

    public void addFieldsToExtendedDataDocument(ElasticsearchSingleDocumentSearchIndex elasticsearchSingleDocumentSearchIndex, XContentBuilder jsonBuilder, Element element, Object o, String tableName, String rowId, List<ExtendedDataMutation> columns, Authorizations authorizations) {

    }
}
