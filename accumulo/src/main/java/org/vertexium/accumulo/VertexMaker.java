package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.vertexium.Authorizations;
import org.vertexium.Vertex;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

import java.util.*;

public class VertexMaker extends ElementMaker<Vertex> {
    private final AccumuloGraph graph;
    private final Map<String, EdgeInfo> outEdges = new HashMap<>();
    private final Map<String, EdgeInfo> inEdges = new HashMap<>();
    private final Set<String> hiddenEdges = new HashSet<>();
    private final List<SoftDeleteEdgeInfo> outSoftDeletes = new ArrayList<>();
    private final List<SoftDeleteEdgeInfo> inSoftDeletes = new ArrayList<>();

    public VertexMaker(AccumuloGraph graph, Iterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        super(graph, row, authorizations);
        this.graph = graph;
    }

    @Override
    protected boolean processColumn(Key key, Value value, String columnFamily, String columnQualifierInflated) {
        if (AccumuloVertex.CF_OUT_EDGE_STRING.equals(columnFamily)) {
            String edgeId = columnQualifierInflated;
            EdgeInfo edgeInfo = EdgeInfo.parse(value, key.getTimestamp(), getGraph().getNameSubstitutionStrategy());
            outEdges.put(edgeId, edgeInfo);
            return true;
        }

        if (AccumuloVertex.CF_IN_EDGE_STRING.equals(columnFamily)) {
            String edgeId = columnQualifierInflated;
            EdgeInfo edgeInfo = EdgeInfo.parse(value, key.getTimestamp(), getGraph().getNameSubstitutionStrategy());
            inEdges.put(edgeId, edgeInfo);
            return true;
        }

        if (AccumuloVertex.CF_OUT_EDGE_HIDDEN_STRING.equals(columnFamily)
                || AccumuloVertex.CF_IN_EDGE_HIDDEN_STRING.equals(columnFamily)) {
            String edgeId = columnQualifierInflated;
            hiddenEdges.add(edgeId);
            return true;
        }

        if (AccumuloVertex.CF_IN_EDGE_SOFT_DELETE_STRING.equals(columnFamily)) {
            String edgeId = columnQualifierInflated;
            inSoftDeletes.add(new SoftDeleteEdgeInfo(edgeId, key.getTimestamp()));
            return true;
        }

        if (AccumuloVertex.CF_OUT_EDGE_SOFT_DELETE_STRING.equals(columnFamily)) {
            String edgeId = columnQualifierInflated;
            outSoftDeletes.add(new SoftDeleteEdgeInfo(edgeId, key.getTimestamp()));
            return true;
        }

        return false;
    }

    @Override
    protected String getVisibilitySignal() {
        return AccumuloVertex.CF_SIGNAL_STRING;
    }

    @Override
    protected Vertex makeElement(boolean includeHidden) {
        if (!includeHidden) {
            for (String edgeId : this.hiddenEdges) {
                this.inEdges.remove(edgeId);
                this.outEdges.remove(edgeId);
            }
        }

        for (SoftDeleteEdgeInfo inSoftDelete : inSoftDeletes) {
            EdgeInfo inEdge = this.inEdges.get(inSoftDelete.getEdgeId());
            if (inEdge != null && inSoftDelete.getTimestamp() >= inEdge.getTimestamp()) {
                this.inEdges.remove(inSoftDelete.getEdgeId());
            }
        }

        for (SoftDeleteEdgeInfo outSoftDelete : outSoftDeletes) {
            EdgeInfo outEdge = this.outEdges.get(outSoftDelete.getEdgeId());
            if (outEdge != null && outSoftDelete.getTimestamp() >= outEdge.getTimestamp()) {
                this.outEdges.remove(outSoftDelete.getEdgeId());
            }
        }

        Iterable<PropertyDeleteMutation> propertyDeleteMutations = null;
        Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = null;
        return new AccumuloVertex(
                this.graph,
                this.getId(),
                this.getVisibility(),
                this.getProperties(includeHidden),
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                this.getHiddenVisibilities(),
                this.inEdges,
                this.outEdges,
                getElementTimestamp(),
                this.getAuthorizations()
        );
    }

    private static class SoftDeleteEdgeInfo {
        private final String edgeId;
        private final long timestamp;

        private SoftDeleteEdgeInfo(String edgeId, long timestamp) {
            this.edgeId = edgeId;
            this.timestamp = timestamp;
        }

        public String getEdgeId() {
            return edgeId;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}
