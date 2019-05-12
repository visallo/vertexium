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
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.AccumuloVertex;
import org.vertexium.accumulo.iterator.VertexIterator;
import org.vertexium.accumulo.iterator.model.VertexElementData;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

public class AccumuloVertexInputFormat extends AccumuloElementInputFormatBase<Vertex> {
    private static VertexIterator vertexIterator;

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
            FetchHints fetchHints = graph.getDefaultFetchHints();
            VertexElementData vertexElementData = getVertexIterator(graph, authorizations).createElementDataFromRows(row);
            if (vertexElementData == null) {
                return null;
            }
            Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(vertexElementData.visibility.toString()));
            Iterable<Property> properties = makePropertiesFromElementData(graph, vertexElementData, graph.toIteratorFetchHints(fetchHints));
            Iterable<PropertyDeleteMutation> propertyDeleteMutations = null;
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = null;
            Iterable<Visibility> hiddenVisibilities = Iterables.transform(vertexElementData.hiddenVisibilities, new Function<Text, Visibility>() {
                @Nullable
                @Override
                public Visibility apply(Text visibilityText) {
                    return AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(visibilityText.toString()));
                }
            });
            Iterable<String> additionalVisibilities = vertexElementData.additionalVisibilities.stream()
                .map(Text::toString)
                .collect(Collectors.toSet());
            ImmutableSet<String> extendedDataTableNames = vertexElementData.extendedTableNames.size() > 0
                ? ImmutableSet.copyOf(vertexElementData.extendedTableNames)
                : null;
            return new AccumuloVertex(
                graph,
                vertexElementData.id.toString(),
                visibility,
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                additionalVisibilities,
                extendedDataTableNames,
                vertexElementData.inEdges,
                vertexElementData.outEdges,
                vertexElementData.timestamp,
                fetchHints,
                authorizations
            );
        } catch (Throwable ex) {
            throw new VertexiumException("Failed to create vertex", ex);
        }
    }

    private static VertexIterator getVertexIterator(AccumuloGraph graph, Authorizations authorizations) {
        if (vertexIterator == null) {
            vertexIterator = new VertexIterator(graph.toIteratorFetchHints(graph.getDefaultFetchHints()), authorizations.getAuthorizations());
        }
        return vertexIterator;
    }
}

