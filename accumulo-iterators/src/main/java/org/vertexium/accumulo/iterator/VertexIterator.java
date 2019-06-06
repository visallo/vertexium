package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.iterator.model.IteratorFetchHints;
import org.vertexium.accumulo.iterator.model.SoftDeleteEdgeInfo;
import org.vertexium.accumulo.iterator.model.VertexElementData;
import org.vertexium.security.Authorizations;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class VertexIterator extends ElementIterator<VertexElementData> {
    public static final String CF_SIGNAL_STRING = "V";
    public static final Text CF_SIGNAL = new Text(CF_SIGNAL_STRING);
    public static final byte[] CF_SIGNAL_BYTES = CF_SIGNAL.getBytes();

    public static final String CF_OUT_EDGE_STRING = "EOUT";
    public static final Text CF_OUT_EDGE = new Text(CF_OUT_EDGE_STRING);
    public static final byte[] CF_OUT_EDGE_BYTES = CF_OUT_EDGE.getBytes();

    public static final String CF_OUT_EDGE_HIDDEN_STRING = "EOUTH";
    public static final Text CF_OUT_EDGE_HIDDEN = new Text(CF_OUT_EDGE_HIDDEN_STRING);
    public static final byte[] CF_OUT_EDGE_HIDDEN_BYTES = CF_OUT_EDGE_HIDDEN.getBytes();

    public static final String CF_OUT_EDGE_SOFT_DELETE_STRING = "EOUTD";
    public static final Text CF_OUT_EDGE_SOFT_DELETE = new Text(CF_OUT_EDGE_SOFT_DELETE_STRING);
    public static final byte[] CF_OUT_EDGE_SOFT_DELETE_BYTES = CF_OUT_EDGE_SOFT_DELETE.getBytes();

    public static final String CF_IN_EDGE_STRING = "EIN";
    public static final Text CF_IN_EDGE = new Text(CF_IN_EDGE_STRING);
    public static final byte[] CF_IN_EDGE_BYTES = CF_IN_EDGE.getBytes();

    public static final String CF_IN_EDGE_HIDDEN_STRING = "EINH";
    public static final Text CF_IN_EDGE_HIDDEN = new Text(CF_IN_EDGE_HIDDEN_STRING);
    public static final byte[] CF_IN_EDGE_HIDDEN_BYTES = CF_IN_EDGE_HIDDEN.getBytes();

    public static final String CF_IN_EDGE_SOFT_DELETE_STRING = "EIND";
    public static final Text CF_IN_EDGE_SOFT_DELETE = new Text(CF_IN_EDGE_SOFT_DELETE_STRING);
    public static final byte[] CF_IN_EDGE_SOFT_DELETE_BYTES = CF_IN_EDGE_SOFT_DELETE.getBytes();

    public VertexIterator() {
        this(null, (String[]) null);
    }

    public VertexIterator(IteratorFetchHints fetchHints, String[] authorizations) {
        super(null, fetchHints, authorizations);
    }

    public VertexIterator(IteratorFetchHints fetchHints, Authorizations authorizations) {
        super(null, fetchHints, authorizations);
    }

    public VertexIterator(SortedKeyValueIterator<Key, Value> source, IteratorFetchHints fetchHints, Authorizations authorizations) {
        super(source, fetchHints, authorizations);
    }

    @Override
    protected Text loadElement() throws IOException {
        Text ret = super.loadElement();
        if (ret != null) {
            removeHiddenAndSoftDeletes();
        }
        return ret;
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
        if (!getFetchHints().isIncludeHidden()) {
            for (Text edgeId : this.getElementData().hiddenEdges) {
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
    protected boolean processColumn(KeyValue keyValue) {
        if (keyValue.columnFamilyEquals(CF_OUT_EDGE_BYTES)) {
            processOutEdge(keyValue);
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_IN_EDGE_BYTES)) {
            processInEdge(keyValue);
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_OUT_EDGE_HIDDEN_BYTES) || keyValue.columnFamilyEquals(CF_IN_EDGE_HIDDEN_BYTES)) {
            Text edgeId = keyValue.takeColumnQualifier();
            if (keyValue.isHidden()) {
                getElementData().hiddenEdges.add(edgeId);
            } else {
                getElementData().hiddenEdges.remove(edgeId);
            }
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_IN_EDGE_SOFT_DELETE_BYTES)) {
            Text edgeId = keyValue.takeColumnQualifier();
            getElementData().inSoftDeletes.add(new SoftDeleteEdgeInfo(edgeId, keyValue.getTimestamp()));
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_OUT_EDGE_SOFT_DELETE_BYTES)) {
            Text edgeId = keyValue.takeColumnQualifier();
            getElementData().outSoftDeletes.add(new SoftDeleteEdgeInfo(edgeId, keyValue.getTimestamp()));
            return true;
        }

        return false;
    }

    private void processOutEdge(KeyValue keyValue) {
        EdgeInfo edgeInfo = EdgeInfo.parse(keyValue.takeValue(), keyValue.getTimestamp());
        if (shouldIncludeOutEdge(edgeInfo)) {
            Text edgeId = keyValue.takeColumnQualifier();
            getElementData().outEdges.add(edgeId, edgeInfo);
        }
    }

    private void processInEdge(KeyValue keyValue) {
        EdgeInfo edgeInfo = EdgeInfo.parse(keyValue.takeValue(), keyValue.getTimestamp());
        if (shouldIncludeInEdge(edgeInfo)) {
            Text edgeId = keyValue.takeColumnQualifier();
            getElementData().inEdges.add(edgeId, edgeInfo);
        }
    }

    private boolean shouldIncludeOutEdge(EdgeInfo edgeInfo) {
        Set<String> labels = getFetchHints().getEdgeLabelsOfEdgeRefsToInclude();
        if (labels != null && labels.contains(edgeInfo.getLabel())) {
            return true;
        }

        return getFetchHints().isIncludeAllEdgeRefs()
            || getFetchHints().isIncludeEdgeLabelsAndCounts()
            || getFetchHints().isIncludeOutEdgeRefs();
    }

    private boolean shouldIncludeInEdge(EdgeInfo edgeInfo) {
        Set<String> labels = getFetchHints().getEdgeLabelsOfEdgeRefsToInclude();
        if (labels != null && labels.contains(edgeInfo.getLabel())) {
            return true;
        }

        return getFetchHints().isIncludeAllEdgeRefs()
            || getFetchHints().isIncludeEdgeLabelsAndCounts()
            || getFetchHints().isIncludeInEdgeRefs();
    }

    @Override
    protected byte[] getVisibilitySignal() {
        return CF_SIGNAL_BYTES;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        if (getSourceIterator() != null) {
            return new VertexIterator(
                getSourceIterator().deepCopy(env),
                getFetchHints(),
                getAuthorizations()
            );
        }
        return new VertexIterator(getFetchHints(), getAuthorizations());
    }

    @Override
    protected String getDescription() {
        return "This iterator encapsulates an entire Vertex into a single Key/Value pair.";
    }

    @Override
    protected VertexElementData createElementData() {
        return new VertexElementData();
    }
}
