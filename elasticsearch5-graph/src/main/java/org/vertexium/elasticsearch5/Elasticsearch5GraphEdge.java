package org.vertexium.elasticsearch5;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.search.SearchHit;
import org.vertexium.*;
import org.vertexium.elasticsearch5.models.Mutation;
import org.vertexium.elasticsearch5.models.UpdateEdgeMutation;
import org.vertexium.elasticsearch5.utils.ProtobufUtils;
import org.vertexium.mutation.ExistingEdgeMutation;
import org.vertexium.property.MutablePropertyImpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class Elasticsearch5GraphEdge extends Elasticsearch5GraphElement implements Edge {
    private final String outVertexId;
    private final String inVertexId;
    private final String label;

    Elasticsearch5GraphEdge(Elasticsearch5Graph graph, SearchHit hit, FetchHints fetchHints, User user) {
        super(graph, hit, fetchHints, user);
        this.outVertexId = hit.getField(FieldNames.OUT_VERTEX_ID).getValue();
        this.inVertexId = hit.getField(FieldNames.IN_VERTEX_ID).getValue();
        this.label = hit.getField(FieldNames.EDGE_LABEL).getValue();
    }

    public Elasticsearch5GraphEdge(
        Elasticsearch5Graph graph,
        String id,
        String outVertexId,
        String inVertexId,
        String label,
        FetchHints fetchHints,
        Iterable<? extends Property> properties,
        ImmutableSet<String> additionalVisibilities,
        Set<Visibility> hiddenVisibilities,
        Visibility visibility,
        User user
    ) {
        super(graph, id, fetchHints, properties, additionalVisibilities, hiddenVisibilities, visibility, user);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
    }

    public static Edge createFromMutations(
        Elasticsearch5Graph graph,
        String id,
        Iterable<SearchHit> mutationResults,
        FetchHints fetchHints,
        User user,
        ScriptService scriptService
    ) {
        AtomicReference<String> outVertexId = new AtomicReference<>();
        AtomicReference<String> inVertexId = new AtomicReference<>();
        AtomicReference<String> label = new AtomicReference<>();
        Set<String> additionalVisibilities = new HashSet<>();
        Set<Visibility> hiddenVisibilities = new HashSet<>();
        List<MutablePropertyImpl> properties = new ArrayList<>();
        AtomicReference<Visibility> visibility = new AtomicReference<>();
        AtomicBoolean created = new AtomicBoolean();
        for (SearchHit searchHit : mutationResults) {
            Mutation mutation = ProtobufUtils.mutationFromField(searchHit.getSource().get(FieldNames.MUTATION_DATA));
            if (mutation == null) {
                throw new VertexiumException("Invalid mutation document. Missing field " + FieldNames.MUTATION_DATA);
            }
            processMutationEdge(
                mutation,
                created,
                outVertexId,
                inVertexId,
                label,
                visibility,
                additionalVisibilities,
                hiddenVisibilities,
                properties,
                fetchHints,
                scriptService
            );
        }

        if (!created.get()) {
            return null;
        }

        return new Elasticsearch5GraphEdge(
            graph,
            id,
            outVertexId.get(),
            inVertexId.get(),
            label.get(),
            fetchHints,
            properties.stream().filter(p -> !user.canRead(p.getVisibility())).collect(Collectors.toList()),
            ImmutableSet.copyOf(additionalVisibilities),
            ImmutableSet.copyOf(hiddenVisibilities),
            visibility.get(),
            user
        );
    }

    private static void processMutationEdge(
        Mutation mutation,
        AtomicBoolean created,
        AtomicReference<String> outVertexId,
        AtomicReference<String> inVertexId,
        AtomicReference<String> label,
        AtomicReference<Visibility> visibility,
        Set<String> additionalVisibilities,
        Set<Visibility> hiddenVisibilities,
        List<MutablePropertyImpl> properties,
        FetchHints fetchHints,
        ScriptService scriptService
    ) {
        switch (mutation.getMutationCase()) {
            case UPDATE_EDGE_MUTATION:
                UpdateEdgeMutation updateEdgeMutation = mutation.getUpdateEdgeMutation();
                created.set(true);
                visibility.set(new Visibility(updateEdgeMutation.getVisibility()));
                outVertexId.set(updateEdgeMutation.getOutVertexId());
                inVertexId.set(updateEdgeMutation.getInVertexId());
                label.set(updateEdgeMutation.getLabel());
                break;

            default:
                processMutation(
                    mutation,
                    created,
                    visibility,
                    additionalVisibilities,
                    hiddenVisibilities,
                    properties,
                    fetchHints,
                    scriptService
                );
                break;
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
                throw new VertexiumException("Invalid direction: " + direction);
        }
    }

    @Override
    public ElementType getElementType() {
        return ElementType.EDGE;
    }

    @Override
    public ExistingEdgeMutation prepareMutation() {
        return new ExistingEdgeMutation(this) {
            @Override
            public Edge save(Authorizations authorizations) {
                User user = authorizations.getUser();
                getGraph().saveMutation(this, null, user);
                getGraph().flush();
                return getGraph().getEdge(getId(), user);
            }

            @Override
            public String save(User user) {
                getGraph().saveMutation(this, null, user);
                return getId();
            }
        };
    }
}
