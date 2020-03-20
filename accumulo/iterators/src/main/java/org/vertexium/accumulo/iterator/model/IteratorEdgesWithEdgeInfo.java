package org.vertexium.accumulo.iterator.model;

public class IteratorEdgesWithEdgeInfo extends EdgesWithEdgeInfo<IteratorEdgeInfo> {
    private EdgeLabels edgeLabels;

    public EdgeLabels getEdgeLabels() {
        return edgeLabels;
    }

    public void clear(EdgeLabels edgeLabels) {
        super.clear();
        this.edgeLabels = edgeLabels;
    }
}
