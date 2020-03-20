package org.vertexium.accumulo.iterator.model;

import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class EdgesWithEdgeInfo<T> extends Edges {
    // We used to use a HashMap here but that was too slow. Replaced with a List since we really don't care about duplicates anyway.
    private List<Map.Entry<Text, T>> pairs = new ArrayList<>();

    public void add(Text edgeId, T edgeInfo) {
        pairs.add(new Pair(edgeId, edgeInfo));
    }

    public void add(String edgeId, T edgeInfo) {
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

    protected void clear() {
        pairs.clear();
    }

    public T get(Text edgeId) {
        int i = indexOf(edgeId);
        if (i >= 0) {
            return pairs.get(i).getValue();
        }
        return null;
    }

    public Iterable<T> getEdgeInfos() {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                final Iterator<Map.Entry<Text, T>> it = pairs.iterator();
                return new Iterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public T next() {
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

    public Iterable<Map.Entry<Text, T>> getEntries() {
        return pairs;
    }

    private class Pair implements Map.Entry<Text, T> {
        private final Text edgeId;
        private final T edgeInfo;

        public Pair(Text edgeId, T edgeInfo) {
            this.edgeId = edgeId;
            this.edgeInfo = edgeInfo;
        }

        @Override
        public Text getKey() {
            return edgeId;
        }

        @Override
        public T getValue() {
            return edgeInfo;
        }

        @Override
        public T setValue(T value) {
            throw new RuntimeException("not supported");
        }
    }
}
