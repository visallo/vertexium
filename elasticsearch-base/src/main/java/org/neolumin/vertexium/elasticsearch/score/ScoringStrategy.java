package org.neolumin.vertexium.elasticsearch.score;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.neolumin.vertexium.*;
import org.neolumin.vertexium.elasticsearch.BulkRequestWithCount;
import org.neolumin.vertexium.elasticsearch.ElasticSearchSearchIndexBase;
import org.neolumin.vertexium.elasticsearch.IndexInfo;
import org.neolumin.vertexium.search.SearchIndex;

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

    public abstract int addElement(ElasticSearchSearchIndexBase searchIndex, Graph graph, BulkRequestWithCount bulkRequestWithCount, IndexInfo indexInfo, Element element, Authorizations authorizations);

    public abstract void addFieldsToElementType(XContentBuilder builder) throws IOException;

    public abstract QueryBuilder updateQuery(QueryBuilder query);

    public List<String> getFieldNames() {
        return new ArrayList<>();
    }
}
