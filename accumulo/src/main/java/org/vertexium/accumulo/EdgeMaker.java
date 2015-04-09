package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.Authorizations;
import org.vertexium.Edge;
import org.vertexium.VertexiumException;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

import java.util.Iterator;
import java.util.Map;

public class EdgeMaker extends ElementMaker<Edge> {
    private static final String VISIBILITY_SIGNAL = AccumuloEdge.CF_SIGNAL.toString();
    private final AccumuloGraph graph;
    private String inVertexId;
    private String outVertexId;
    private String label;
    private long timestamp;

    public EdgeMaker(
            AccumuloGraph graph,
            Iterator<Map.Entry<Key, Value>> row,
            Authorizations authorizations
    ) {
        super(graph, row, authorizations);
        this.graph = graph;
    }

    @Override
    protected void processColumn(Key key, Value value) {
        Text columnFamily = getColumnFamily(key);
        Text columnQualifier = getColumnQualifier(key);

        if (AccumuloEdge.CF_SIGNAL.compareTo(columnFamily) == 0) {
            this.label = columnQualifier.toString();
            this.timestamp = key.getTimestamp();
            return;
        }

        if (AccumuloEdge.CF_IN_VERTEX.compareTo(columnFamily) == 0) {
            this.inVertexId = columnQualifier.toString();
            return;
        }

        if (AccumuloEdge.CF_OUT_VERTEX.compareTo(columnFamily) == 0) {
            this.outVertexId = columnQualifier.toString();
        }
    }

    @Override
    protected String getIdFromRowKey(String rowKey) {
        if (rowKey.startsWith(AccumuloConstants.EDGE_ROW_KEY_PREFIX)) {
            return rowKey.substring(AccumuloConstants.EDGE_ROW_KEY_PREFIX.length());
        }
        throw new VertexiumException("Invalid row key for edge: " + rowKey);
    }

    @Override
    protected String getVisibilitySignal() {
        return VISIBILITY_SIGNAL;
    }

    @Override
    protected Edge makeElement(boolean includeHidden) {
        String newEdgeLabel = null;
        Iterable<PropertyDeleteMutation> propertyDeleteMutations = null;
        Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = null;
        return new AccumuloEdge(
                this.graph,
                this.getId(),
                this.outVertexId,
                this.inVertexId,
                this.label,
                newEdgeLabel,
                this.getVisibility(),
                this.getProperties(includeHidden),
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                this.getHiddenVisibilities(),
                this.getAuthorizations(),
                this.timestamp
        );
    }
}
