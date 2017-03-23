package org.vertexium.elasticsearch2.score;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.vertexium.*;
import org.vertexium.elasticsearch2.Elasticsearch2SearchIndex;
import org.vertexium.elasticsearch2.IndexInfo;
import org.vertexium.elasticsearch2.utils.GetResponseUtil;
import org.vertexium.search.SearchIndex;

import java.io.IOException;
import java.util.List;

public class EdgeCountScoringStrategy extends ScoringStrategy {
    private final EdgeCountScoringStrategyConfiguration config;

    public EdgeCountScoringStrategy(GraphConfiguration config) {
        super(config);
        this.config = new EdgeCountScoringStrategyConfiguration(config);
    }

    public EdgeCountScoringStrategyConfiguration getConfig() {
        return config;
    }

    @Override
    public void addElement(SearchIndex searchIndex, Graph graph, Element element, Authorizations authorizations) {
        if (!getConfig().isUpdateEdgeBoost()) {
            return;
        }
        if (!(element instanceof Edge)) {
            return;
        }

        Element vOut = ((Edge) element).getVertex(Direction.OUT, authorizations);
        if (vOut != null) {
            searchIndex.addElement(graph, vOut, authorizations);
        }

        Element vIn = ((Edge) element).getVertex(Direction.IN, authorizations);
        if (vIn != null) {
            searchIndex.addElement(graph, vIn, authorizations);
        }
    }

    @Override
    public int addElement(Elasticsearch2SearchIndex searchIndex, Graph graph, BulkRequest bulkRequest, IndexInfo indexInfo, Element element, Authorizations authorizations) {
        int totalCount = 0;

        if (!getConfig().isUpdateEdgeBoost()) {
            return totalCount;
        }
        if (!(element instanceof Edge)) {
            return totalCount;
        }

        Element vOut = ((Edge) element).getVertex(Direction.OUT, authorizations);
        if (vOut != null) {
            searchIndex.addElementToBulkRequest(graph, bulkRequest, indexInfo, vOut, authorizations);
            totalCount++;
        }

        Element vIn = ((Edge) element).getVertex(Direction.IN, authorizations);
        if (vIn != null) {
            searchIndex.addElementToBulkRequest(graph, bulkRequest, indexInfo, vIn, authorizations);
            totalCount++;
        }

        return totalCount;
    }

    @Override
    public void addFieldsToElementType(XContentBuilder builder) throws IOException {
        builder
                .startObject(EdgeCountScoringStrategyConfiguration.IN_EDGE_COUNT_FIELD_NAME).field("type", "integer").field("store", "true").endObject()
                .startObject(EdgeCountScoringStrategyConfiguration.OUT_EDGE_COUNT_FIELD_NAME).field("type", "integer").field("store", "true").endObject()
        ;
    }

    @Override
    public List<String> getFieldNames() {
        List<String> fieldNames = super.getFieldNames();
        fieldNames.add(EdgeCountScoringStrategyConfiguration.IN_EDGE_COUNT_FIELD_NAME);
        fieldNames.add(EdgeCountScoringStrategyConfiguration.OUT_EDGE_COUNT_FIELD_NAME);
        return fieldNames;
    }

    @Override
    public QueryBuilder updateQuery(QueryBuilder query) {
        if (!getConfig().isUseEdgeBoost()) {
            return query;
        }

        Script script = new Script(
                getConfig().getScoreFormula(),
                ScriptService.ScriptType.INLINE,
                null,
                ImmutableMap.of(
                        "inEdgeMultiplier", getConfig().getInEdgeBoost(),
                        "outEdgeMultiplier", getConfig().getOutEdgeBoost()
                ));
        ScoreFunctionBuilder scoreFunction = ScoreFunctionBuilders.scriptFunction(script);

        return QueryBuilders.functionScoreQuery(query, scoreFunction);
    }

    @Override
    public boolean addFieldsToVertexDocument(SearchIndex searchIndex, XContentBuilder jsonBuilder, Vertex vertex, GetResponse existingDocument, Authorizations authorizations) throws IOException {
        if (existingDocument != null && !getConfig().isUpdateEdgeBoost()) {
            return false;
        }

        boolean changed = false;

        int inEdgeCount = vertex.getEdgeCount(Direction.IN, authorizations);
        Long existingInEdgeCount = existingDocument == null ? null : GetResponseUtil.getFieldValueLong(existingDocument, EdgeCountScoringStrategyConfiguration.IN_EDGE_COUNT_FIELD_NAME);
        if (existingInEdgeCount == null || existingInEdgeCount.intValue() != inEdgeCount) {
            jsonBuilder.field(EdgeCountScoringStrategyConfiguration.IN_EDGE_COUNT_FIELD_NAME, inEdgeCount);
            changed = true;
        } else {
            jsonBuilder.field(EdgeCountScoringStrategyConfiguration.IN_EDGE_COUNT_FIELD_NAME, existingInEdgeCount.intValue());
        }

        int outEdgeCount = vertex.getEdgeCount(Direction.OUT, authorizations);
        Long existingOutEdgeCount = existingDocument == null ? null : GetResponseUtil.getFieldValueLong(existingDocument, EdgeCountScoringStrategyConfiguration.OUT_EDGE_COUNT_FIELD_NAME);
        if (existingOutEdgeCount == null || existingOutEdgeCount.intValue() != outEdgeCount) {
            jsonBuilder.field(EdgeCountScoringStrategyConfiguration.OUT_EDGE_COUNT_FIELD_NAME, outEdgeCount);
            changed = true;
        } else {
            jsonBuilder.field(EdgeCountScoringStrategyConfiguration.OUT_EDGE_COUNT_FIELD_NAME, existingOutEdgeCount.intValue());
        }

        return changed;
    }

    @Override
    public boolean addFieldsToEdgeDocument(SearchIndex searchIndex, XContentBuilder jsonBuilder, Edge edge, GetResponse existingDocument, Authorizations authorizations) throws IOException {
        return false;
    }
}
