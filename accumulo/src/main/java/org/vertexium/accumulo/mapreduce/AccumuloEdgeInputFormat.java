package org.vertexium.accumulo.mapreduce;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.vertexium.*;
import org.vertexium.accumulo.AccumuloEdge;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.iterator.EdgeIterator;
import org.vertexium.accumulo.iterator.model.EdgeElementData;
import org.vertexium.accumulo.iterator.model.FetchHint;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.Map;

public class AccumuloEdgeInputFormat extends AccumuloElementInputFormatBase<Edge> {
    private static EdgeIterator edgeIterator = new EdgeIterator(AccumuloGraph.toIteratorFetchHints(org.vertexium.FetchHint.ALL));

    public static void setInputInfo(Job job, AccumuloGraph graph, String instanceName, String zooKeepers, String principal, AuthenticationToken token, String[] authorizations) throws AccumuloSecurityException {
        String tableName = graph.getEdgesTableName();
        setInputInfo(job, instanceName, zooKeepers, principal, token, authorizations, tableName);
    }

    @Override
    protected Edge createElementFromRow(AccumuloGraph graph, PeekingIterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        try {
            EnumSet<FetchHint> fetchHints = AccumuloGraph.toIteratorFetchHints(org.vertexium.FetchHint.ALL);
            EdgeElementData edgeElementData = edgeIterator.createElementDataFromRows(row);
            if (edgeElementData == null) {
                return null;
            }
            Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(edgeElementData.visibility.toString()));
            Iterable<Property> properties = makePropertiesFromElementData(graph, edgeElementData, fetchHints);
            Iterable<PropertyDeleteMutation> propertyDeleteMutations = null;
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = null;
            Iterable<Visibility> hiddenVisibilities = Iterables.transform(edgeElementData.hiddenVisibilities, new Function<Text, Visibility>() {
                @Nullable
                @Override
                public Visibility apply(Text visibilityText) {
                    return AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(visibilityText.toString()));
                }
            });
            return new AccumuloEdge(
                    graph,
                    edgeElementData.id.toString(),
                    edgeElementData.outVertexId.toString(),
                    edgeElementData.inVertexId.toString(),
                    edgeElementData.label.toString(),
                    null,
                    visibility,
                    properties,
                    propertyDeleteMutations,
                    propertySoftDeleteMutations,
                    hiddenVisibilities,
                    edgeElementData.timestamp,
                    authorizations
            );
        } catch (Throwable ex) {
            throw new VertexiumException("Failed to create vertex", ex);
        }
    }
}

