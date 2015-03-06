package org.neolumin.vertexium.accumulo;

import org.neolumin.vertexium.util.LookAheadIterable;

import java.util.Collection;
import java.util.Iterator;

class GetVertexIdsIterable extends LookAheadIterable<EdgeInfo, String> {
    private final Collection<EdgeInfo> edgeInfos;
    private final String[] labels;

    public GetVertexIdsIterable(Collection<EdgeInfo> edgeInfos, String[] labels) {
        this.edgeInfos = edgeInfos;
        this.labels = labels;
    }

    @Override
    protected boolean isIncluded(EdgeInfo edgeInfo, String vertexId) {
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
    protected String convert(EdgeInfo edgeInfo) {
        return edgeInfo.getVertexId();
    }

    @Override
    protected Iterator<EdgeInfo> createIterator() {
        return edgeInfos.iterator();
    }
}
