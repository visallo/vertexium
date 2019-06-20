package org.vertexium.elasticsearch5;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.search.SearchHit;
import org.vertexium.Element;
import org.vertexium.Property;
import org.vertexium.*;
import org.vertexium.elasticsearch5.models.*;
import org.vertexium.elasticsearch5.utils.ProtobufUtils;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.query.*;
import org.vertexium.util.StreamUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.vertexium.util.StreamUtils.toIterable;

public abstract class Elasticsearch5GraphElement extends ElementBase implements Element {
    private final Elasticsearch5Graph graph;
    private final FetchHints fetchHints;
    private final User user;
    private final ImmutableSet<String> additionalVisibilities;
    private final Iterable<? extends Property> properties;
    private final String id;
    private final Visibility visibility;
    private final long timestamp;
    private final Set<Visibility> hiddenVisibilities;
    private ImmutableSet<String> extendedDataTableNames;

    public Elasticsearch5GraphElement(Elasticsearch5Graph graph, SearchHit hit, FetchHints fetchHints, User user) {
        this.graph = graph;
        this.fetchHints = fetchHints;
        this.user = user;
        this.additionalVisibilities = Elasticsearch5GraphVertexiumObject.readAdditionalVisibilitiesFromSearchHit(hit);
        this.properties = new LazyProperties(
            hit.getField(FieldNames.PROPERTIES_DATA),
            fetchHints,
            graph.getStreamingPropertyValueService(),
            graph.getScriptService(),
            user
        );
        this.id = hit.getField(FieldNames.ELEMENT_ID).getValue();
        this.visibility = new Visibility(hit.getField(FieldNames.ELEMENT_VISIBILITY).getValue());
        this.hiddenVisibilities = readHiddenVisibilitiesFromSearchHit(hit);
        this.timestamp = 0L; // TODO timestamp???
    }

    public Elasticsearch5GraphElement(
        Elasticsearch5Graph graph,
        String id,
        FetchHints fetchHints,
        Iterable<? extends Property> properties,
        ImmutableSet<String> additionalVisibilities,
        Set<Visibility> hiddenVisibilities,
        Visibility visibility,
        User user
    ) {
        this.graph = graph;
        this.fetchHints = fetchHints;
        this.user = user;
        this.id = id;
        this.timestamp = 0L; // TODO timestamp???
        this.properties = properties;
        this.additionalVisibilities = additionalVisibilities;
        this.visibility = visibility;
        this.hiddenVisibilities = hiddenVisibilities;
    }

    protected static void processMutation(
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
            case SET_PROPERTY_MUTATION:
                applySetPropertyMutation(
                    properties,
                    mutation.getTimestamp(),
                    mutation.getSetPropertyMutation(),
                    fetchHints,
                    scriptService
                );
                break;

            case MARK_PROPERTY_HIDDEN_MUTATION:
                applyMarkPropertyHiddenMutation(properties, mutation.getMarkPropertyHiddenMutation());
                break;

            case MARK_PROPERTY_VISIBLE_MUTATION:
                applyMarkPropertyVisibleMutation(properties, mutation.getMarkPropertyVisibleMutation());
                break;

            case PROPERTY_SOFT_DELETE_MUTATION:
                applyPropertySoftDeleteMutation(properties, mutation.getPropertySoftDeleteMutation());
                break;

            case PROPERTY_DELETE_MUTATION:
                applyPropertyDeleteMutation(properties, mutation.getPropertyDeleteMutation());
                break;

            case MARK_HIDDEN_MUTATION:
                applyMarkHiddenMutation(hiddenVisibilities, mutation.getMarkHiddenMutation());
                break;

            case ALTER_ELEMENT_VISIBILITY_MUTATION:
                applyAlterElementVisibilityMutation(visibility, mutation.getAlterElementVisibilityMutation());
                break;

            case DELETE_MUTATION:
                applyDeleteMutation(created, mutation.getDeleteMutation());
                break;

            default:
                // TODO handle all mutation types
                throw new VertexiumException("Unhandled mutation: " + mutation.getMutationCase());
        }
    }

    private static void applyDeleteMutation(AtomicBoolean created, DeleteMutation deleteMutation) {
        created.set(false);
    }

    private static void applyAlterElementVisibilityMutation(
        AtomicReference<Visibility> visibility,
        AlterElementVisibilityMutation alterElementVisibilityMutation
    ) {
        visibility.set(new Visibility(alterElementVisibilityMutation.getVisibility()));
    }

    private static void applyMarkHiddenMutation(
        Set<Visibility> hiddenVisibilities,
        MarkHiddenMutation markHiddenMutation
    ) {
        hiddenVisibilities.add(new Visibility(markHiddenMutation.getVisibility()));
    }

    private static void applyPropertyDeleteMutation(
        List<MutablePropertyImpl> properties,
        PropertyDeleteMutation propertyDeleteMutation
    ) {
        properties.removeIf(prop -> doesPropertyMatch(
            prop,
            propertyDeleteMutation.getKey(),
            propertyDeleteMutation.getName(),
            propertyDeleteMutation.getVisibility()
        ));
    }

    private static void applyPropertySoftDeleteMutation(
        List<MutablePropertyImpl> properties,
        PropertySoftDeleteMutation propertySoftDeleteMutation
    ) {
        properties.removeIf(prop -> doesPropertyMatch(
            prop,
            propertySoftDeleteMutation.getKey(),
            propertySoftDeleteMutation.getName(),
            propertySoftDeleteMutation.getVisibility()
        ));
    }

    private static void applyMarkPropertyVisibleMutation(
        List<MutablePropertyImpl> properties,
        MarkPropertyVisibleMutation markPropertyVisibleMutation
    ) {
        getProperties(
            properties,
            markPropertyVisibleMutation.getKey(),
            markPropertyVisibleMutation.getName(),
            markPropertyVisibleMutation.getPropertyVisibility()
        ).forEach(property -> {
            property.removeHiddenVisibility(new Visibility(markPropertyVisibleMutation.getVisibility()));
        });
    }

    private static void applyMarkPropertyHiddenMutation(
        List<MutablePropertyImpl> properties,
        MarkPropertyHiddenMutation markPropertyHiddenMutation
    ) {
        getProperties(
            properties,
            markPropertyHiddenMutation.getKey(),
            markPropertyHiddenMutation.getName(),
            markPropertyHiddenMutation.getPropertyVisibility()
        ).forEach(property -> {
            property.addHiddenVisibility(new Visibility(markPropertyHiddenMutation.getVisibility()));
        });
    }

    private static Stream<MutablePropertyImpl> getProperties(
        List<MutablePropertyImpl> properties,
        String key,
        String name,
        String visibility
    ) {
        return properties.stream()
            .filter(prop -> doesPropertyMatch(prop, key, name, visibility));
    }

    private static boolean doesPropertyMatch(Property prop, String key, String name, String visibility) {
        return (key == null || prop.getKey().equals(key))
            && (name == null || prop.getName().equals(name))
            && (visibility == null || prop.getVisibility().getVisibilityString().equals(visibility));
    }

    private static void applySetPropertyMutation(
        List<MutablePropertyImpl> properties,
        long timestamp,
        SetPropertyMutation setPropertyMutation,
        FetchHints fetchHints,
        ScriptService scriptService
    ) {
        properties.add(new MutablePropertyImpl(
            setPropertyMutation.getKey(),
            setPropertyMutation.getName(),
            scriptService.valueToJavaObject(setPropertyMutation.getValue()),
            scriptService.protobufMetadataToVertexium(setPropertyMutation.getMetadataList(), fetchHints),
            timestamp,
            new HashSet<>(),
            new Visibility(setPropertyMutation.getVisibility()),
            fetchHints
        ));
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public Stream<HistoricalEvent> getHistoricalEvents(HistoricalEventId after, HistoricalEventsFetchHints fetchHints, User user) {
        return graph.getMutationStore().getHistoricalEvents(after, fetchHints, user);
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        return hiddenVisibilities;
    }

    @Override
    public ImmutableSet<String> getExtendedDataTableNames() {
        if (!getFetchHints().isIncludeExtendedDataTableNames()) {
            throw new VertexiumMissingFetchHintException(getFetchHints(), "includeExtendedDataTableNames");
        }
        if (extendedDataTableNames != null) {
            return extendedDataTableNames;
        }
        QueryResultsIterable<ExtendedDataRow> results = graph.query(new Elasticsearch5GraphAuthorizations(getUser().getAuthorizations()))
            .has(ExtendedDataRow.ELEMENT_TYPE, getElementType())
            .has(ExtendedDataRow.ELEMENT_ID, getId())
            .limit(0L)
            .addAggregation(new TermsAggregation("tableNames", ExtendedDataRow.TABLE_NAME))
            .extendedDataRows();
        TermsResult aggResults = results.getAggregationResult("tableNames", TermsResult.class);
        ImmutableSet.Builder<String> tableNamesBuilder = new ImmutableSet.Builder<>();
        for (TermsBucket bucket : aggResults.getBuckets()) {
            tableNamesBuilder.add((String) bucket.getKey());
        }
        extendedDataTableNames = tableNamesBuilder.build();
        return extendedDataTableNames;
    }

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName, FetchHints fetchHints) {
        return new ExtendedDataQueryableIterable(
            getGraph(),
            this,
            tableName,
            toIterable(graph.getExtendedData(getElementType(), getId(), tableName, fetchHints, getUser()))
        );
    }

    private static ImmutableSet<Visibility> readHiddenVisibilitiesFromSearchHit(SearchHit hit) {
        HiddenData hiddenVisibilitiesData = ProtobufUtils.hiddenDataFromField(hit.getField(FieldNames.HIDDEN_VISIBILITY_DATA));
        return hiddenVisibilitiesData.getHiddenDataList().stream()
            .map(esMarkHiddenData -> new Visibility(esMarkHiddenData.getVisibility()))
            .collect(StreamUtils.toImmutableSet());
    }

    @Override
    public ImmutableSet<String> getAdditionalVisibilities() {
        return additionalVisibilities;
    }

    @Override
    public Elasticsearch5Graph getGraph() {
        return graph;
    }

    @Override
    public User getUser() {
        return user;
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Property> getProperties() {
        if (!getFetchHints().isIncludeProperties()) {
            throw new VertexiumMissingFetchHintException(getFetchHints(), "includeProperties");
        }
        return (Iterable<Property>) properties;
    }
}
