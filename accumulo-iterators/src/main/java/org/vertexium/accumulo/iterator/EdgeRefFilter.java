package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.iterator.util.SetOfStringsEncoder;

import java.io.IOException;
import java.util.*;

public class EdgeRefFilter extends Filter {
    private static final String SETTING_VERTEX_IDS = "vertexId";
    private Set<String> vertexIdsSet;
    private List<Text> nonVisibleEdges = Arrays.asList(
        VertexIterator.CF_IN_EDGE_HIDDEN,
        VertexIterator.CF_IN_EDGE_SOFT_DELETE,
        VertexIterator.CF_OUT_EDGE_HIDDEN,
        VertexIterator.CF_OUT_EDGE_SOFT_DELETE);

    public static void setVertexIds(IteratorSetting settings, Set<String> vertexIdsSet) {
        settings.addOption(SETTING_VERTEX_IDS, SetOfStringsEncoder.encodeToString(vertexIdsSet));
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        this.vertexIdsSet = SetOfStringsEncoder.decodeFromString(options.get(SETTING_VERTEX_IDS));
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        EdgeRefFilter edgeRefFilter = (EdgeRefFilter) super.deepCopy(env);
        edgeRefFilter.vertexIdsSet = new HashSet<>(this.vertexIdsSet);
        return edgeRefFilter;
    }

    @Override
    public boolean accept(Key k, Value v) {
        Text columnFamily = k.getColumnFamily();
        if (columnFamily.equals(VertexIterator.CF_IN_EDGE) || columnFamily.equals(VertexIterator.CF_OUT_EDGE)) {
            EdgeInfo edgeInfo = new EdgeInfo(v.get(), k.getColumnVisibility(), k.getTimestamp());
            return vertexIdsSet.contains(edgeInfo.getVertexId());
        }
        return nonVisibleEdges.contains(columnFamily);
    }
}
