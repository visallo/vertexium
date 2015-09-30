package org.vertexium.accumulo.iterator;

import com.google.common.collect.Lists;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EdgeRefFilter extends Filter {
    private Set<String> vertexIdsSet;
    private List<Text> nonVisibleEdges = Lists.newArrayList(
            VertexIterator.CF_IN_EDGE_HIDDEN,
            VertexIterator.CF_IN_EDGE_SOFT_DELETE,
            VertexIterator.CF_OUT_EDGE_HIDDEN,
            VertexIterator.CF_OUT_EDGE_SOFT_DELETE);
    private static final String SETTING_VERTEX_IDS = "vertexId";

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
            EdgeInfo edgeInfo = new EdgeInfo(v.get(), k.getTimestamp());
            return vertexIdsSet.contains(edgeInfo.getVertexId());
        }
        return nonVisibleEdges.contains(columnFamily);
    }
}
