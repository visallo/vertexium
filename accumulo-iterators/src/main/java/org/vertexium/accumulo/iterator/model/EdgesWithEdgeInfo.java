package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EdgesWithEdgeInfo extends Edges {
    // We used to use a HashMap here but that was too slow. Replaced with a List since we really don't care about duplicates anyway.
    private List<Map.Entry<Text, EdgeInfo>> pairs = new ArrayList<>();

    public void add(Text edgeId, EdgeInfo edgeInfo) {
        pairs.add(new Pair(edgeId, edgeInfo));
    }

    public void add(String edgeId, EdgeInfo edgeInfo) {
        add(new Text(edgeId), edgeInfo);
    }

    public void remove(Text edgeId) {
        int i = indexOf(edgeId);
        if (i >= 0) {
            pairs.remove(i);
        }
    }

    private int indexOf(Text edgeId) {
        for (int i = 0; i < pairs.size(); i++) {
            if (pairs.get(i).getKey().equals(edgeId)) {
                return i;
            }
        }
        return -1;
    }

    public void remove(String edgeId) {
        remove(new Text(edgeId));
    }

    public void clear() {
        pairs.clear();
    }

    public EdgeInfo get(Text edgeId) {
        int i = indexOf(edgeId);
        if (i >= 0) {
            return pairs.get(i).getValue();
        }
        return null;
    }

    public Iterable<EdgeInfo> getEdgeInfos() {
        return new Iterable<EdgeInfo>() {
            @Override
            public Iterator<EdgeInfo> iterator() {
                final Iterator<Map.Entry<Text, EdgeInfo>> it = pairs.iterator();
                return new Iterator<EdgeInfo>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public EdgeInfo next() {
                        return it.next().getValue();
                    }

                    @Override
                    public void remove() {
                        throw new RuntimeException("not supported");
                    }
                };
            }
        };
    }

    public Iterable<Map.Entry<Text, EdgeInfo>> getEntries() {
        return pairs;
    }

    public Iterable<Map.Entry<String, EdgeInfo>> getEntriesWithStringKeys() {
        return new Iterable<Map.Entry<String, EdgeInfo>>() {
            @Override
            public Iterator<Map.Entry<String, EdgeInfo>> iterator() {
                final Iterator<Map.Entry<Text, EdgeInfo>> it = pairs.iterator();
                return new Iterator<Map.Entry<String, EdgeInfo>>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public Map.Entry<String, EdgeInfo> next() {
                        final Map.Entry<Text, EdgeInfo> pair = it.next();
                        return new Map.Entry<String, EdgeInfo>() {
                            @Override
                            public String getKey() {
                                return pair.getKey().toString();
                            }

                            @Override
                            public EdgeInfo getValue() {
                                return pair.getValue();
                            }

                            @Override
                            public EdgeInfo setValue(EdgeInfo value) {
                                throw new RuntimeException("not supported");
                            }
                        };
                    }

                    @Override
                    public void remove() {
                        throw new RuntimeException("not supported");
                    }
                };
            }
        };
    }

    private static class Pair implements Map.Entry<Text, EdgeInfo> {
        private final Text edgeId;
        private final EdgeInfo edgeInfo;

        public Pair(Text edgeId, EdgeInfo edgeInfo) {
            this.edgeId = edgeId;
            this.edgeInfo = edgeInfo;
        }

        @Override
        public Text getKey() {
            return edgeId;
        }

        @Override
        public EdgeInfo getValue() {
            return edgeInfo;
        }

        @Override
        public EdgeInfo setValue(EdgeInfo value) {
            throw new RuntimeException("not supported");
        }
    }
}
