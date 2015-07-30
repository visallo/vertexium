package org.vertexium.accumulo.iterator.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class EdgesWithCount extends Edges {
    private Map<String, Integer> edgeCounts = new HashMap<>();

    public void add(String label, int count) {
        edgeCounts.put(label, count);
    }

    public Set<String> getLabels() {
        return edgeCounts.keySet();
    }
}
