package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EdgesWithEdgeInfo extends Edges {
    private Map<Text, EdgeInfo> edges = new HashMap<>();

    public void add(Text edgeId, EdgeInfo edgeInfo) {
        edges.put(edgeId, edgeInfo);
    }

    public void add(String edgeId, EdgeInfo edgeInfo) {
        add(new Text(edgeId), edgeInfo);
    }

    public Map<Text, EdgeInfo> getEdges() {
        return edges;
    }

    public void remove(Text edgeId) {
        edges.remove(edgeId);
    }

    public void remove(String edgeId) {
        remove(new Text(edgeId));
    }

    public void clear() {
        edges.clear();
    }

    public EdgeInfo get(Text edgeId) {
        return edges.get(edgeId);
    }

    public Iterable<Map.Entry<String, EdgeInfo>> getEdgeInfos() {
        return new Iterable<Map.Entry<String, EdgeInfo>>() {
            @Override
            public Iterator<Map.Entry<String, EdgeInfo>> iterator() {
                final Iterator<Map.Entry<Text, EdgeInfo>> it = getEdges().entrySet().iterator();
                return new Iterator<Map.Entry<String, EdgeInfo>>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public Map.Entry<String, EdgeInfo> next() {
                        final Map.Entry<Text, EdgeInfo> entry = it.next();
                        return new Map.Entry<String, EdgeInfo>() {
                            public String keyString;

                            @Override
                            public String getKey() {
                                if (keyString == null) {
                                    keyString = entry.getKey().toString();
                                }
                                return keyString;
                            }

                            @Override
                            public EdgeInfo getValue() {
                                return entry.getValue();
                            }

                            @Override
                            public EdgeInfo setValue(EdgeInfo value) {
                                throw new VertexiumAccumuloIteratorException("Not supported");
                            }
                        };
                    }
                };
            }
        };
    }
}
