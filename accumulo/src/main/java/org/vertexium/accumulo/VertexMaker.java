package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.Authorizations;
import org.vertexium.Vertex;
import org.vertexium.VertexiumException;

import java.util.*;

public class VertexMaker extends ElementMaker<Vertex> {
    private static final String VISIBILITY_SIGNAL = AccumuloVertex.CF_SIGNAL.toString();

    private final AccumuloGraph graph;
    private final Map<String, EdgeInfo> outEdges = new HashMap<>();
    private final Map<String, EdgeInfo> inEdges = new HashMap<>();
    private final Set<String> hiddenEdges = new HashSet<>();
    private long timestamp;

    public VertexMaker(AccumuloGraph graph, Iterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        super(graph, row, authorizations);
        this.graph = graph;
    }

    @Override
    protected void processColumn(Key key, Value value) {
        Text columnFamily = getColumnFamily(key);
        Text columnQualifier = getColumnQualifier(key);

        if (AccumuloVertex.CF_SIGNAL.compareTo(columnFamily) == 0) {
            this.timestamp = key.getTimestamp();
            return;
        }

        if (AccumuloVertex.CF_OUT_EDGE_HIDDEN.compareTo(columnFamily) == 0
                || AccumuloVertex.CF_IN_EDGE_HIDDEN.compareTo(columnFamily) == 0) {
            String edgeId = columnQualifier.toString();
            hiddenEdges.add(edgeId);
            return;
        }

        if (AccumuloVertex.CF_OUT_EDGE.compareTo(columnFamily) == 0) {
            String edgeId = columnQualifier.toString();
            EdgeInfo edgeInfo = EdgeInfo.parse(value);
            outEdges.put(edgeId, edgeInfo);
            return;
        }

        if (AccumuloVertex.CF_IN_EDGE.compareTo(columnFamily) == 0) {
            String edgeId = columnQualifier.toString();
            EdgeInfo edgeInfo = EdgeInfo.parse(value);
            inEdges.put(edgeId, edgeInfo);
            return;
        }
    }

    @Override
    protected String getIdFromRowKey(String rowKey) throws VertexiumException {
        if (rowKey.startsWith(AccumuloConstants.VERTEX_ROW_KEY_PREFIX)) {
            return rowKey.substring(AccumuloConstants.VERTEX_ROW_KEY_PREFIX.length());
        }
        throw new VertexiumException("Invalid row key for vertex: " + rowKey);
    }

    @Override
    protected String getVisibilitySignal() {
        return VISIBILITY_SIGNAL;
    }

    @Override
    protected Vertex makeElement(boolean includeHidden) {
        if (!includeHidden) {
            for (String edgeId : this.hiddenEdges) {
                this.inEdges.remove(edgeId);
                this.outEdges.remove(edgeId);
            }
        }

        return new AccumuloVertex(
                this.graph,
                this.getId(),
                this.getVisibility(),
                this.getProperties(includeHidden),
                null,
                this.getHiddenVisibilities(),
                this.inEdges,
                this.outEdges,
                this.getAuthorizations(),
                timestamp
        );
    }

}
