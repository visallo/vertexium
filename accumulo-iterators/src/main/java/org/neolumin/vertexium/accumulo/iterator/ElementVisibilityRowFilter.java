package org.neolumin.vertexium.accumulo.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowFilter;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;

public class ElementVisibilityRowFilter extends RowFilter {
    public static final String OPT_FILTER_VERTICES = "vertex.enabled";
    public static final String OPT_FILTER_EDGES = "edge.enabled";

    // must match org.neolumin.vertexium.accumulo.AccumuloVertex.CF_SIGNAL
    private static final Text VERTEX_CF_SIGNAL = new Text("V");

    // must match org.neolumin.vertexium.accumulo.AccumuloEdge.CF_SIGNAL
    private static final Text EDGE_CF_SIGNAL = new Text("E");

    boolean filterVertices;
    boolean filterEdges;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        this.filterVertices = options.get(OPT_FILTER_VERTICES) != null;
        this.filterEdges = options.get(OPT_FILTER_EDGES) != null;

        if (!this.filterVertices && !this.filterEdges) {
            throw new IllegalArgumentException(OPT_FILTER_VERTICES + " and/or " + OPT_FILTER_EDGES + " must be set to a non-null value.");
        }

        super.init(source, options, env);
    }

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key, Value> rowIterator) throws IOException {
        while (rowIterator.hasTop()) {
            Key key = rowIterator.getTopKey();
            if (isVisible(key)) {
                return true;
            }
            rowIterator.next();
        }
        return false;
    }

    private boolean isVisible(Key key) {
        return (this.filterVertices && key.compareColumnFamily(VERTEX_CF_SIGNAL) == 0) ||
                (this.filterEdges && key.compareColumnFamily(EDGE_CF_SIGNAL) == 0);
    }

}
