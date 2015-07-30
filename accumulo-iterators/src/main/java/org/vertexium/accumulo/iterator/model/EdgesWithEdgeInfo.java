package org.vertexium.accumulo.iterator.model;

import java.util.HashMap;
import java.util.Map;

public class EdgesWithEdgeInfo extends Edges {
    private Map<String, EdgeInfo> edges = new HashMap<>();

    public void add(String edgeId, EdgeInfo edgeInfo) {
        edges.put(edgeId, edgeInfo);
    }

    public Map<String, EdgeInfo> getEdges() {
        return edges;
    }

    public void remove(String edgeId) {
        edges.remove(edgeId);
    }

    public void clear() {
        edges.clear();
    }

    public EdgeInfo get(String edgeId) {
        return edges.get(edgeId);
    }
}
