package org.vertexium.accumulo.mapreduce;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
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
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.stream.Collectors;

public class AccumuloEdgeInputFormat extends AccumuloElementInputFormatBase<Edge> {
    private static EdgeIterator edgeIterator;

    public static void setInputInfo(Job job, AccumuloGraph graph, String instanceName, String zooKeepers, String principal, AuthenticationToken token, String[] authorizations) throws AccumuloSecurityException {
        String tableName = graph.getEdgesTableName();
        setInputInfo(job, instanceName, zooKeepers, principal, token, authorizations, tableName);
    }

    @Override
    protected Edge createElementFromRow(
        AccumuloGraph graph,
        PeekingIterator<Map.Entry<Key, Value>> row,
        Authorizations authorizations
    ) {
        try {
            FetchHints fetchHints = graph.getDefaultFetchHints();
            EdgeElementData edgeElementData = getEdgeIterator(graph, authorizations).createElementDataFromRows(row);
            if (edgeElementData == null) {
                return null;
            }
            Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(edgeElementData.visibility.toString()));
            Iterable<Property> properties = makePropertiesFromElementData(graph, edgeElementData, graph.toIteratorFetchHints(fetchHints));
            Iterable<PropertyDeleteMutation> propertyDeleteMutations = null;
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = null;
            Iterable<Visibility> hiddenVisibilities = Iterables.transform(edgeElementData.hiddenVisibilities, new Function<Text, Visibility>() {
                @Nullable
                @Override
                public Visibility apply(Text visibilityText) {
                    return AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(visibilityText.toString()));
                }
            });
            Iterable<String> additionalVisibilities = edgeElementData.additionalVisibilities.stream()
                .map(Text::toString)
                .collect(Collectors.toSet());
            ImmutableSet<String> extendedDataTableNames = edgeElementData.extendedTableNames.size() > 0
                ? ImmutableSet.copyOf(edgeElementData.extendedTableNames)
                : null;
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
                additionalVisibilities,
                extendedDataTableNames,
                edgeElementData.timestamp,
                fetchHints,
                authorizations
            );
        } catch (Throwable ex) {
            throw new VertexiumException("Failed to create vertex", ex);
        }
    }

    private EdgeIterator getEdgeIterator(AccumuloGraph graph, Authorizations authorizations) {
        if (edgeIterator == null) {
            edgeIterator = new EdgeIterator(graph.toIteratorFetchHints(graph.getDefaultFetchHints()), authorizations.getAuthorizations());
        }
        return edgeIterator;
    }
}

