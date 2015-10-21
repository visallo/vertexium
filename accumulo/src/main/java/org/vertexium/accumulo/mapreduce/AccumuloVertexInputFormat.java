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
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.AccumuloVertex;
import org.vertexium.accumulo.iterator.VertexIterator;
import org.vertexium.accumulo.iterator.model.FetchHint;
import org.vertexium.accumulo.iterator.model.VertexElementData;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;

public class AccumuloVertexInputFormat extends AccumuloElementInputFormatBase<Vertex> {
    private static VertexIterator vertexIterator = new VertexIterator(AccumuloGraph.toIteratorFetchHints(org.vertexium.FetchHint.ALL));

    public static void setInputInfo(Job job, AccumuloGraph graph, String instanceName, String zooKeepers, String principal, AuthenticationToken token, String[] authorizations) throws AccumuloSecurityException {
        String tableName = graph.getVerticesTableName();
        setInputInfo(job, instanceName, zooKeepers, principal, token, authorizations, tableName);
    }

    @Override
    protected Vertex createElementFromRow(AccumuloGraph graph, PeekingIterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        return createVertex(graph, row, authorizations);
    }

    public static Vertex createVertex(AccumuloGraph graph, Iterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        try {
            EnumSet<FetchHint> fetchHints = AccumuloGraph.toIteratorFetchHints(org.vertexium.FetchHint.ALL);
            VertexElementData vertexElementData = vertexIterator.createElementDataFromRows(row);
            if (vertexElementData == null) {
                return null;
            }
            Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(vertexElementData.visibility.toString()));
            Iterable<Property> properties = makePropertiesFromElementData(graph, vertexElementData, fetchHints);
            Iterable<PropertyDeleteMutation> propertyDeleteMutations = null;
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = null;
            Iterable<Visibility> hiddenVisibilities = Iterables.transform(vertexElementData.hiddenVisibilities, new Function<Text, Visibility>() {
                @Nullable
                @Override
                public Visibility apply(Text visibilityText) {
                    return AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(visibilityText.toString()));
                }
            });
            return new AccumuloVertex(
                    graph,
                    vertexElementData.id.toString(),
                    visibility,
                    properties,
                    propertyDeleteMutations,
                    propertySoftDeleteMutations,
                    hiddenVisibilities,
                    vertexElementData.inEdges,
                    vertexElementData.outEdges,
                    vertexElementData.timestamp,
                    authorizations
            );
        } catch (Throwable ex) {
            throw new VertexiumException("Failed to create vertex", ex);
        }
    }
}

