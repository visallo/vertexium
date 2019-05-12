package org.vertexium.accumulo;

import com.google.common.collect.ImmutableSet;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.EdgeIterator;
import org.vertexium.accumulo.iterator.model.ElementData;
import org.vertexium.accumulo.util.DataInputStreamUtils;
import org.vertexium.mutation.ExistingEdgeMutation;
import org.vertexium.util.StreamUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class AccumuloEdge extends AccumuloElement implements Edge {
    public static final Text CF_SIGNAL = EdgeIterator.CF_SIGNAL;
    public static final Text CF_OUT_VERTEX = EdgeIterator.CF_OUT_VERTEX;
    public static final Text CF_IN_VERTEX = EdgeIterator.CF_IN_VERTEX;
    private final String outVertexId;
    private final String inVertexId;
    private final String label;

    public AccumuloEdge(
        Graph graph,
        String id,
        String outVertexId,
        String inVertexId,
        String label,
        Visibility visibility,
        Iterable<Property> properties,
        Iterable<Visibility> hiddenVisibilities,
        Iterable<Visibility> additionalVisibilities,
        ImmutableSet<String> extendedDataTableNames,
        long timestamp,
        FetchHints fetchHints,
        User user
    ) {
        super(
            graph,
            id,
            visibility,
            properties,
            hiddenVisibilities,
            additionalVisibilities,
            extendedDataTableNames,
            timestamp,
            fetchHints,
            user
        );
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
    }

    public static Edge createFromIteratorValue(
        AccumuloGraph graph,
        Key key,
        Value value,
        FetchHints fetchHints,
        User user
    ) {
        try {
            String edgeId;
            Visibility vertexVisibility;
            Iterable<Property> properties;
            Iterable<Visibility> hiddenVisibilities;
            long timestamp;

            ByteArrayInputStream bain = new ByteArrayInputStream(value.get());
            final DataInputStream in = new DataInputStream(bain);
            DataInputStreamUtils.decodeHeader(in, ElementData.TYPE_ID_EDGE);
            edgeId = DataInputStreamUtils.decodeString(in);
            timestamp = in.readLong();
            vertexVisibility = new Visibility(DataInputStreamUtils.decodeString(in));

            hiddenVisibilities = DataInputStreamUtils.decodeStringSet(in).stream()
                .map(Visibility::new)
                .collect(Collectors.toSet());

            ImmutableSet<Visibility> additionalVisibilities = DataInputStreamUtils.decodeStringSet(in).stream()
                .map(Visibility::new)
                .collect(StreamUtils.toImmutableSet());

            List<MetadataEntry> metadataEntries = DataInputStreamUtils.decodeMetadataEntries(in);
            properties = DataInputStreamUtils.decodeProperties(graph, in, metadataEntries, fetchHints);
            ImmutableSet<String> extendedDataTableNames = DataInputStreamUtils.decodeStringSet(in);
            String inVertexId = DataInputStreamUtils.decodeString(in);
            String outVertexId = DataInputStreamUtils.decodeString(in);
            String label = graph.getNameSubstitutionStrategy().inflate(DataInputStreamUtils.decodeString(in));

            return new AccumuloEdge(
                graph,
                edgeId,
                outVertexId,
                inVertexId,
                label,
                vertexVisibility,
                properties,
                hiddenVisibilities,
                additionalVisibilities,
                extendedDataTableNames,
                timestamp,
                fetchHints,
                user
            );
        } catch (IOException ex) {
            throw new VertexiumException("Could not read vertex", ex);
        }
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String getVertexId(Direction direction) {
        switch (direction) {
            case OUT:
                return outVertexId;
            case IN:
                return inVertexId;
            default:
                throw new IllegalArgumentException("Unexpected direction: " + direction);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public ExistingEdgeMutation prepareMutation() {
        return new ExistingEdgeMutation(this) {
            @Override
            public String save(User user) {
                saveExistingElementMutation(this, user);
                return getId();
            }

            @Override
            public Edge save(Authorizations authorizations) {
                saveExistingElementMutation(this, authorizations.getUser());
                getGraph().flush();
                return getGraph().getEdge(getId(), getElement().getFetchHints(), authorizations);
            }
        };
    }
}
