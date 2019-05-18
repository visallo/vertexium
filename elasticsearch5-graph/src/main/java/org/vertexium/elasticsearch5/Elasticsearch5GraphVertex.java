package org.vertexium.elasticsearch5;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.search.SearchHit;
import org.vertexium.*;
import org.vertexium.elasticsearch5.models.Mutation;
import org.vertexium.elasticsearch5.models.UpdateVertexMutation;
import org.vertexium.elasticsearch5.utils.ProtobufUtils;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExistingElementMutationBase;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.query.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.stream;

public class Elasticsearch5GraphVertex extends Elasticsearch5GraphElement implements Vertex {
    Elasticsearch5GraphVertex(Elasticsearch5Graph graph, SearchHit hit, FetchHints fetchHints, User user) {
        super(graph, hit, fetchHints, user);
    }

    public Elasticsearch5GraphVertex(
        Elasticsearch5Graph graph,
        String id,
        FetchHints fetchHints,
        Iterable<? extends Property> properties,
        ImmutableSet<String> additionalVisibilities,
        Set<Visibility> hiddenVisibilities,
        Visibility visibility,
        User user
    ) {
        super(graph, id, fetchHints, properties, additionalVisibilities, hiddenVisibilities, visibility, user);
    }

    public static Elasticsearch5GraphVertex createFromMutations(
        Elasticsearch5Graph graph,
        String id,
        Iterable<SearchHit> mutationResults,
        FetchHints fetchHints,
        User user,
        ScriptService scriptService
    ) {
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
            processMutationVertex(
                mutation,
                created,
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

        return new Elasticsearch5GraphVertex(
            graph,
            id,
            fetchHints,
            properties.stream().filter(p -> !user.canRead(p.getVisibility())).collect(Collectors.toList()),
            ImmutableSet.copyOf(additionalVisibilities),
            ImmutableSet.copyOf(hiddenVisibilities),
            visibility.get(),
            user
        );
    }

    private static void processMutationVertex(
        Mutation mutation,
        AtomicBoolean created,
        AtomicReference<Visibility> visibility,
        Set<String> additionalVisibilities,
        Set<Visibility> hiddenVisibilities,
        List<MutablePropertyImpl> properties,
        FetchHints fetchHints,
        ScriptService scriptService
    ) {
        switch (mutation.getMutationCase()) {
            case UPDATE_VERTEX_MUTATION:
                UpdateVertexMutation updateVertexMutation = mutation.getUpdateVertexMutation();
                created.set(true);
                visibility.set(new Visibility(updateVertexMutation.getVisibility()));
                break;

            default:
                processMutation(
                    mutation,
                    created, visibility,
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
    public ExistingElementMutation<Vertex> prepareMutation() {
        return new ExistingElementMutationBase<Vertex>(this) {
            @Override
            public Vertex save(Authorizations authorizations) {
                User user = authorizations.getUser();
                getGraph().saveMutation(this, null, user);
                getGraph().flush();
                return getGraph().getVertex(getId(), user);
            }

            @Override
            public String save(User user) {
                getGraph().saveMutation(this, null, user);
                return getId();
            }
        };
    }

    @Override
    public Stream<Edge> getEdges(
        Direction direction,
        String[] labels,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        return getEdges(
            null,
            direction,
            labels,
            fetchHints,
            user
        );
    }

    @Override
    public Stream<Edge> getEdges(
        Vertex otherVertex,
        Direction direction,
        String[] labels,
        FetchHints fetchHints,
        User user
    ) {
        // TODO we don't actually need this fetch hint since this call results in a query. How should this be handled?
        getFetchHints().validateHasEdgeFetchHints(direction, labels);

        // TODO convert to new query
        Query query = getGraph().query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations()));
        switch (direction) {
            case BOTH:
                String[] ids = otherVertex == null ? new String[]{getId()} : new String[]{otherVertex.getId(), getId()};
                query = query
                    .has(Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME, Contains.IN, ids);
                break;

            case OUT:
                query = query.has(Edge.OUT_VERTEX_ID_PROPERTY_NAME, getId());
                if (otherVertex != null) {
                    query = query.has(Edge.IN_VERTEX_ID_PROPERTY_NAME, otherVertex.getId());
                }
                break;

            case IN:
                query = query.has(Edge.IN_VERTEX_ID_PROPERTY_NAME, getId());
                if (otherVertex != null) {
                    query = query.has(Edge.OUT_VERTEX_ID_PROPERTY_NAME, otherVertex.getId());
                }
                break;
        }
        if (labels != null) {
            query = query.has(Edge.LABEL_PROPERTY_NAME, Contains.IN, labels);
        }
        return stream(query.edges(fetchHints));
    }

    @Override
    public EdgesSummary getEdgesSummary(User user) {
        // TODO number of terms (edge labels) size limit?
        // TODO convert to new query
        QueryResultsIterable<Edge> outQueryResults = getGraph().query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations()))
            .has(Edge.OUT_VERTEX_ID_PROPERTY_NAME, getId())
            .addAggregation(new TermsAggregation("label", Edge.LABEL_PROPERTY_NAME))
            .limit(0L)
            .edges(getFetchHints());
        QueryResultsIterable<Edge> inQueryResults = getGraph().query(new Elasticsearch5GraphAuthorizations(user.getAuthorizations()))
            .has(Edge.IN_VERTEX_ID_PROPERTY_NAME, getId())
            .addAggregation(new TermsAggregation("label", Edge.LABEL_PROPERTY_NAME))
            .limit(0L)
            .edges(getFetchHints());

        TermsResult outResults = outQueryResults.getAggregationResult("label", TermsResult.class);
        TermsResult inResults = inQueryResults.getAggregationResult("label", TermsResult.class);

        Map<String, Integer> outEdgeCountsByLabels = new HashMap<>();
        Map<String, Integer> inEdgeCountsByLabels = new HashMap<>();

        for (TermsBucket bucket : outResults.getBuckets()) {
            outEdgeCountsByLabels.put((String) bucket.getKey(), (int) bucket.getCount());
        }
        for (TermsBucket bucket : inResults.getBuckets()) {
            inEdgeCountsByLabels.put((String) bucket.getKey(), (int) bucket.getCount());
        }

        return new EdgesSummary(outEdgeCountsByLabels, inEdgeCountsByLabels);
    }

    @Override
    public Stream<EdgeInfo> getEdgeInfos(Direction direction, String[] labels, User user) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return internalGetEdgeInfo(direction, labels, getFetchHints(), user)
            .filter(o -> {
                if (!getFetchHints().isIncludeEdgeRefLabel(o.getLabel())) {
                    return false;
                }
                if (labels == null) {
                    return true;
                } else {
                    for (String label : labels) {
                        if (o.getLabel().equals(label)) {
                            return true;
                        }
                    }
                    return false;
                }
            });
    }

    private Stream<EdgeInfo> internalGetEdgeInfo(Direction direction, String[] labels, FetchHints fetchHints, User user) {
        // TODO faster way to do this?
        return getEdges(direction, labels, fetchHints, user)
            .map(edge -> new EdgeInfo() {
                @Override
                public String getEdgeId() {
                    return edge.getId();
                }

                @Override
                public String getLabel() {
                    return edge.getLabel();
                }

                @Override
                public String getVertexId() {
                    return edge.getOtherVertexId(Elasticsearch5GraphVertex.this.getId());
                }

                @Override
                public Direction getDirection() {
                    if (edge.getVertexId(Direction.OUT).equals(Elasticsearch5GraphVertex.this.getId())) {
                        return Direction.OUT;
                    } else {
                        return Direction.IN;
                    }
                }

                @Override
                public Visibility getVisibility() {
                    return edge.getVisibility();
                }
            });
    }

    @Override
    public ElementType getElementType() {
        return ElementType.VERTEX;
    }
}
