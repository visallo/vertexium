package org.vertexium.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.Authorizations;
import org.vertexium.Edge;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

import java.util.Iterator;
import java.util.Map;

public class EdgeMaker extends ElementMaker<Edge> {
    private final AccumuloGraph graph;
    private String inVertexId;
    private String outVertexId;
    private String label;

    public EdgeMaker(
            AccumuloGraph graph,
            Iterator<Map.Entry<Key, Value>> row,
            Authorizations authorizations
    ) {
        super(graph, row, authorizations);
        this.graph = graph;
    }

    @Override
    protected boolean processColumn(Key key, Value value, String columnFamily, String columnQualifierInflated) {
        if (AccumuloEdge.CF_IN_VERTEX_STRING.compareTo(columnFamily) == 0) {
            this.inVertexId = columnQualifierInflated;
            return true;
        }

        if (AccumuloEdge.CF_OUT_VERTEX_STRING.compareTo(columnFamily) == 0) {
            this.outVertexId = columnQualifierInflated;
            return true;
        }

        return false;
    }

    @Override
    protected void processSignalColumn(Text columnQualifier) {
        super.processSignalColumn(columnQualifier);
        this.label = inflateColumnQualifier(columnQualifier);
    }

    @Override
    protected String getVisibilitySignal() {
        return AccumuloEdge.CF_SIGNAL_STRING;
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
                getElementTimestamp(),
                this.getAuthorizations()
        );
    }
}
