package org.vertexium.accumulo;

import org.vertexium.accumulo.models.AccumuloEdgeInfo;
import org.vertexium.util.LookAheadIterable;

import java.util.Iterator;

class GetVertexIdsIterable extends LookAheadIterable<AccumuloEdgeInfo, String> {
    private final Iterable<AccumuloEdgeInfo> edgeInfos;
    private final String[] labels;

    public GetVertexIdsIterable(Iterable<AccumuloEdgeInfo> edgeInfos, String[] labels) {
        this.edgeInfos = edgeInfos;
        this.labels = labels;
    }

    @Override
    protected boolean isIncluded(AccumuloEdgeInfo edgeInfo, String vertexId) {
        if (labels == null || labels.length == 0) {
            return true;
        }
        for (String label : labels) {
            if (edgeInfo.getLabel().equals(label)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected String convert(AccumuloEdgeInfo edgeInfo) {
        return edgeInfo.getVertexId();
    }

    @Override
    protected Iterator<AccumuloEdgeInfo> createIterator() {
        return edgeInfos.iterator();
    }
}
