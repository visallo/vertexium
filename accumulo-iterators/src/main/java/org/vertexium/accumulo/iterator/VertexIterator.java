package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.iterator.model.FetchHint;
import org.vertexium.accumulo.iterator.model.SoftDeleteEdgeInfo;
import org.vertexium.accumulo.iterator.model.VertexElementData;

import java.util.EnumSet;
import java.util.List;

public class VertexIterator extends ElementIterator<VertexElementData> {
    public static final String CF_SIGNAL_STRING = "V";
    public static final Text CF_SIGNAL = new Text(CF_SIGNAL_STRING);
    public static final String CF_OUT_EDGE_STRING = "EOUT";
    public static final Text CF_OUT_EDGE = new Text(CF_OUT_EDGE_STRING);
    public static final String CF_OUT_EDGE_HIDDEN_STRING = "EOUTH";
    public static final Text CF_OUT_EDGE_HIDDEN = new Text(CF_OUT_EDGE_HIDDEN_STRING);
    public static final String CF_OUT_EDGE_SOFT_DELETE_STRING = "EOUTD";
    public static final Text CF_OUT_EDGE_SOFT_DELETE = new Text(CF_OUT_EDGE_SOFT_DELETE_STRING);
    public static final String CF_IN_EDGE_STRING = "EIN";
    public static final Text CF_IN_EDGE = new Text(CF_IN_EDGE_STRING);
    public static final String CF_IN_EDGE_HIDDEN_STRING = "EINH";
    public static final Text CF_IN_EDGE_HIDDEN = new Text(CF_IN_EDGE_HIDDEN_STRING);
    public static final String CF_IN_EDGE_SOFT_DELETE_STRING = "EIND";
    public static final Text CF_IN_EDGE_SOFT_DELETE = new Text(CF_IN_EDGE_SOFT_DELETE_STRING);

    public VertexIterator() {
        this(FetchHint.ALL);
    }

    public VertexIterator(EnumSet<FetchHint> fetchHints) {
        super(null, fetchHints);
    }

    public VertexIterator(SortedKeyValueIterator<Key, Value> source, EnumSet<FetchHint> fetchHints) {
        super(source, fetchHints);
    }

    @Override
    protected boolean populateElementData(List<Key> keys, List<Value> values) {
        boolean ret = super.populateElementData(keys, values);
        if (ret) {
            removeHiddenAndSoftDeletes();
        }
        return ret;
    }

    private void removeHiddenAndSoftDeletes() {
        if (!getFetchHints().contains(FetchHint.INCLUDE_HIDDEN)) {
            for (String edgeId : this.getElementData().hiddenEdges) {
                this.getElementData().inEdges.remove(edgeId);
                this.getElementData().outEdges.remove(edgeId);
            }
        }

        for (SoftDeleteEdgeInfo inSoftDelete : this.getElementData().inSoftDeletes) {
            EdgeInfo inEdge = this.getElementData().inEdges.get(inSoftDelete.getEdgeId());
            if (inEdge != null && inSoftDelete.getTimestamp() >= inEdge.getTimestamp()) {
                this.getElementData().inEdges.remove(inSoftDelete.getEdgeId());
            }
        }

        for (SoftDeleteEdgeInfo outSoftDelete : this.getElementData().outSoftDeletes) {
            EdgeInfo outEdge = this.getElementData().outEdges.get(outSoftDelete.getEdgeId());
            if (outEdge != null && outSoftDelete.getTimestamp() >= outEdge.getTimestamp()) {
                this.getElementData().outEdges.remove(outSoftDelete.getEdgeId());
            }
        }
    }

    @Override
    protected boolean processColumn(Key key, Value value, Text columnFamily, Text columnQualifier) {
        if (CF_OUT_EDGE.equals(columnFamily)) {
            Text edgeId = key.getColumnQualifier();
            EdgeInfo edgeInfo = EdgeInfo.parse(value, key.getTimestamp());
            getElementData().outEdges.add(edgeId.toString(), edgeInfo);
            return true;
        }

        if (CF_IN_EDGE.equals(columnFamily)) {
            Text edgeId = key.getColumnQualifier();
            EdgeInfo edgeInfo = EdgeInfo.parse(value, key.getTimestamp());
            getElementData().inEdges.add(edgeId.toString(), edgeInfo);
            return true;
        }

        if (CF_OUT_EDGE_HIDDEN.equals(columnFamily)
                || CF_IN_EDGE_HIDDEN.equals(columnFamily)) {
            String edgeId = key.getColumnQualifier().toString();
            getElementData().hiddenEdges.add(edgeId);
            return true;
        }

        if (CF_IN_EDGE_SOFT_DELETE.equals(columnFamily)) {
            String edgeId = key.getColumnQualifier().toString();
            getElementData().inSoftDeletes.add(new SoftDeleteEdgeInfo(edgeId, key.getTimestamp()));
            return true;
        }

        if (CF_OUT_EDGE_SOFT_DELETE.equals(columnFamily)) {
            String edgeId = key.getColumnQualifier().toString();
            getElementData().outSoftDeletes.add(new SoftDeleteEdgeInfo(edgeId, key.getTimestamp()));
            return true;
        }

        return false;
    }

    @Override
    protected Text getVisibilitySignal() {
        return CF_SIGNAL;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        if (sourceIter != null) {
            return new VertexIterator(sourceIter.deepCopy(env), getFetchHints());
        }
        return new VertexIterator(getFetchHints());
    }

    @Override
    protected VertexElementData createElementData() {
        return new VertexElementData();
    }
}
