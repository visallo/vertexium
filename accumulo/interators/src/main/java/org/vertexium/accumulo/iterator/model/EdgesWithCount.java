package org.vertexium.accumulo.iterator.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class EdgesWithCount extends Edges {
    private Map<String, Integer> edgeCountsByLabelName = new HashMap<>();

    public void add(String label, int count) {
        edgeCountsByLabelName.put(label, count);
    }

    public Set<String> getLabels() {
        return edgeCountsByLabelName.keySet();
    }

    public Map<String, Integer> getEdgeCountsByLabelName() {
        return edgeCountsByLabelName;
    }
}
