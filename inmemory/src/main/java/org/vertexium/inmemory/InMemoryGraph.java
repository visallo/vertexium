package org.vertexium.inmemory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.vertexium.*;
import org.vertexium.event.*;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.id.IdGenerator;
import org.vertexium.inmemory.mutations.AlterEdgeLabelMutation;
import org.vertexium.inmemory.mutations.AlterVisibilityMutation;
import org.vertexium.inmemory.mutations.EdgeSetupMutation;
import org.vertexium.inmemory.mutations.ElementTimestampMutation;
import org.vertexium.mutation.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.query.GraphQuery;
import org.vertexium.query.MultiVertexQuery;
import org.vertexium.query.SimilarToGraphQuery;
import org.vertexium.search.IndexHint;
import org.vertexium.search.SearchIndex;
import org.vertexium.util.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.Preconditions.checkNotNull;
import static org.vertexium.util.StreamUtils.stream;

public class InMemoryGraph extends GraphBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(InMemoryGraph.class);
    protected static final InMemoryGraphConfiguration DEFAULT_CONFIGURATION =
        new InMemoryGraphConfiguration(new HashMap<>());
    private final Set<String> validAuthorizations = new HashSet<>();
    private final InMemoryVertexTable vertices;
    private final InMemoryEdgeTable edges;
    private final InMemoryExtendedDataTable extendedDataTable;
    private final GraphMetadataStore graphMetadataStore;

    protected InMemoryGraph(InMemoryGraphConfiguration configuration) {
        this(
            configuration,
            new InMemoryVertexTable(),
            new InMemoryEdgeTable(),
            new MapInMemoryExtendedDataTable()
        );
    }

    protected InMemoryGraph(InMemoryGraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        this(
            configuration,
            idGenerator,
            searchIndex,
            new InMemoryVertexTable(),
            new InMemoryEdgeTable(),
            new MapInMemoryExtendedDataTable()
        );
    }

    protected InMemoryGraph(
        InMemoryGraphConfiguration configuration,
        InMemoryVertexTable vertices,
        InMemoryEdgeTable edges,
        InMemoryExtendedDataTable extendedDataTable
    ) {
        super(configuration);
        this.vertices = vertices;
        this.edges = edges;
        this.extendedDataTable = extendedDataTable;
        this.graphMetadataStore = newGraphMetadataStore();
    }

    protected InMemoryGraph(
        InMemoryGraphConfiguration configuration,
        IdGenerator idGenerator,
        SearchIndex searchIndex,
        InMemoryVertexTable vertices,
        InMemoryEdgeTable edges,
        InMemoryExtendedDataTable extendedDataTable
    ) {
        super(configuration, idGenerator, searchIndex);
        this.vertices = vertices;
        this.edges = edges;
        this.extendedDataTable = extendedDataTable;
        this.graphMetadataStore = newGraphMetadataStore();
    }

    protected GraphMetadataStore newGraphMetadataStore() {
        return new InMemoryGraphMetadataStore();
    }

    @SuppressWarnings("unused")
    public static InMemoryGraph create() {
        return create(DEFAULT_CONFIGURATION);
    }

    public static InMemoryGraph create(InMemoryGraphConfiguration config) {
        InMemoryGraph graph = new InMemoryGraph(config);
        graph.setup();
        return graph;
    }

    @SuppressWarnings("unused")
    public static InMemoryGraph create(Map<String, Object> config) {
        return create(new InMemoryGraphConfiguration(config));
    }

    @SuppressWarnings("unused")
    public static InMemoryGraph create(InMemoryGraphConfiguration config, IdGenerator idGenerator, SearchIndex searchIndex) {
        InMemoryGraph graph = new InMemoryGraph(config, idGenerator, searchIndex);
        graph.setup();
        return graph;
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        return new VertexBuilder(vertexId, visibility) {
            @Override
            public Vertex save(Authorizations authorizations) {
                return saveVertex(authorizations.getUser());
            }

            @Override
            public String save(User user) {
                return saveVertex(user).getId();
            }

            private Vertex saveVertex(User user) {
                addValidAuthorizations(user.getAuthorizations());
                boolean isNew = false;
                InMemoryTableElement vertexTableElement = InMemoryGraph.this.vertices.getTableElement(getId());
                if (vertexTableElement == null) {
                    isNew = true;
                    vertices.append(
                        getId(),
                        new AlterVisibilityMutation(timestampLong, getVisibility(), null),
                        new ElementTimestampMutation(timestampLong)
                    );
                } else {
                    if (vertexTableElement.getVisibility().equals(getVisibility())) {
                        vertices.append(getId(), new ElementTimestampMutation(timestampLong));
                    } else {
                        vertices.append(getId(), new AlterVisibilityMutation(timestampLong, getVisibility(), null), new ElementTimestampMutation(timestampLong));
                    }
                }
                InMemoryVertex vertex = InMemoryGraph.this.vertices.get(InMemoryGraph.this, getId(), FetchHints.ALL_INCLUDING_HIDDEN, user);
                if (isNew && hasEventListeners()) {
                    fireGraphEvent(new AddVertexEvent(InMemoryGraph.this, vertex));
                }
                vertex.updatePropertiesInternal(this);

                // to more closely simulate how accumulo works. add a potentially sparse (in case of an update) vertex to the search index.
                if (getIndexHint() != IndexHint.DO_NOT_INDEX) {
                    updateElementAndExtendedDataInSearchIndex(vertex, this, user);
                }

                return vertex;
            }
        };
    }

    <T extends Element> void updateElementAndExtendedDataInSearchIndex(
        Element element,
        ElementMutation<T> elementMutation,
        User user
    ) {
        if (elementMutation instanceof ExistingElementMutation) {
            getSearchIndex().updateElement(this, (ExistingElementMutation<? extends Element>) elementMutation, user);
        } else {
            getSearchIndex().addElement(
                this,
                element,
                stream(elementMutation.getAdditionalVisibilities())
                    .map(AdditionalVisibilityAddMutation::getAdditionalVisibility)
                    .collect(Collectors.toSet()),
                stream(elementMutation.getAdditionalVisibilityDeletes())
                    .map(AdditionalVisibilityDeleteMutation::getAdditionalVisibility)
                    .collect(Collectors.toSet()),
                user
            );
        }
        getSearchIndex().addElementExtendedData(
            InMemoryGraph.this,
            element,
            elementMutation.getExtendedData(),
            elementMutation.getAdditionalExtendedDataVisibilities(),
            elementMutation.getAdditionalExtendedDataVisibilityDeletes(),
            user
        );
        for (ExtendedDataDeleteMutation m : elementMutation.getExtendedDataDeletes()) {
            getSearchIndex().deleteExtendedData(
                InMemoryGraph.this,
                element,
                m.getTableName(),
                m.getRow(),
                m.getColumnName(),
                m.getKey(),
                m.getVisibility(),
                user
            );
        }
    }

    <T extends Element> void updateElementAndExtendedDataInSearchIndex(
        Element element,
        ElementMutation<T> elementMutation,
        Authorizations authorizations
    ) {
        if (elementMutation instanceof ExistingElementMutation) {
            getSearchIndex().updateElement(this, (ExistingElementMutation<? extends Element>) elementMutation, authorizations);
        } else {
            getSearchIndex().addElement(this, element, authorizations);
        }
        getSearchIndex().addElementExtendedData(
            InMemoryGraph.this,
            element,
            elementMutation.getExtendedData(),
            elementMutation.getAdditionalExtendedDataVisibilities(),
            elementMutation.getAdditionalExtendedDataVisibilityDeletes(),
            authorizations
        );
        for (ExtendedDataDeleteMutation m : elementMutation.getExtendedDataDeletes()) {
            getSearchIndex().deleteExtendedData(
                InMemoryGraph.this,
                element,
                m.getTableName(),
                m.getRow(),
                m.getColumnName(),
                m.getKey(),
                m.getVisibility(),
                authorizations
            );
        }
    }

    private void addValidAuthorizations(String[] authorizations) {
        Collections.addAll(this.validAuthorizations, authorizations);
    }

    @Override
    public Stream<Vertex> getVertices(FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        return this.vertices.getAll(InMemoryGraph.this, fetchHints, endTime, user)
            .map(v -> v);
    }

    protected void validateAuthorizations(User user) {
        for (String auth : user.getAuthorizations()) {
            if (!this.validAuthorizations.contains(auth)) {
                throw new SecurityVertexiumException("Invalid authorizations", user);
            }
        }
    }

    @Override
    public void deleteVertex(Vertex vertex, Authorizations authorizations) {
        if (!((InMemoryVertex) vertex).canRead(authorizations)) {
            return;
        }

        List<Edge> edgesToDelete = IterableUtils.toList(vertex.getEdges(Direction.BOTH, authorizations));
        for (Edge edgeToDelete : edgesToDelete) {
            deleteEdge(edgeToDelete, authorizations);
        }

        deleteAllExtendedDataForElement(vertex, authorizations);

        this.vertices.remove(vertex.getId());
        getSearchIndex().deleteElement(this, vertex, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new DeleteVertexEvent(this, vertex));
        }
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Object eventData, Authorizations authorizations) {

    }

    @Override
    public void softDeleteVertex(Vertex vertex, Long timestamp, Object eventData, Authorizations authorizations) {
        if (!((InMemoryVertex) vertex).canRead(authorizations)) {
            return;
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        for (Property property : vertex.getProperties()) {
            vertex.softDeleteProperty(property.getKey(), property.getName(), property.getVisibility(), eventData, authorizations);
        }

        List<Edge> edgesToSoftDelete = IterableUtils.toList(vertex.getEdges(Direction.BOTH, authorizations));
        for (Edge edgeToSoftDelete : edgesToSoftDelete) {
            softDeleteEdge(edgeToSoftDelete, timestamp, eventData, authorizations);
        }

        this.vertices.getTableElement(vertex.getId()).appendSoftDeleteMutation(timestamp, eventData);

        getSearchIndex().deleteElement(this, vertex, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new SoftDeleteVertexEvent(this, vertex, eventData));
        }
    }

    @Override
    public void markVertexHidden(Vertex vertex, Visibility visibility, Object eventData, Authorizations authorizations) {
        if (!((InMemoryVertex) vertex).canRead(authorizations)) {
            return;
        }

        List<Edge> edgesToMarkHidden = IterableUtils.toList(vertex.getEdges(Direction.BOTH, authorizations));
        for (Edge edgeToMarkHidden : edgesToMarkHidden) {
            markEdgeHidden(edgeToMarkHidden, visibility, eventData, authorizations);
        }

        this.vertices.getTableElement(vertex.getId()).appendMarkHiddenMutation(visibility, eventData);
        refreshVertexInMemoryTableElement(vertex);
        getSearchIndex().markElementHidden(this, vertex, visibility, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new MarkHiddenVertexEvent(this, vertex, eventData));
        }
    }

    @Override
    public void markVertexVisible(Vertex vertex, Visibility visibility, Object eventData, Authorizations authorizations) {
        if (!((InMemoryVertex) vertex).canRead(authorizations)) {
            return;
        }

        List<Edge> edgesToMarkVisible = IterableUtils.toList(vertex.getEdges(Direction.BOTH, FetchHints.ALL_INCLUDING_HIDDEN, authorizations));
        for (Edge edgeToMarkVisible : edgesToMarkVisible) {
            markEdgeVisible(edgeToMarkVisible, visibility, eventData, authorizations);
        }

        this.vertices.getTableElement(vertex.getId()).appendMarkVisibleMutation(visibility, eventData);
        refreshVertexInMemoryTableElement(vertex);
        getSearchIndex().markElementVisible(this, vertex, visibility, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new MarkVisibleVertexEvent(this, vertex, eventData));
        }
    }

    public void markPropertyHidden(
        InMemoryElement element,
        InMemoryTableElement inMemoryTableElement,
        Property property,
        Long timestamp,
        Visibility visibility,
        Object data,
        User user
    ) {
        if (!element.canRead(user)) {
            return;
        }

        Property hiddenProperty = inMemoryTableElement.appendMarkPropertyHiddenMutation(
            property.getKey(),
            property.getName(),
            property.getVisibility(),
            timestamp,
            visibility,
            data,
            user
        );

        getSearchIndex().markPropertyHidden(this, element, property, visibility, user);

        if (hiddenProperty != null && hasEventListeners()) {
            fireGraphEvent(new MarkHiddenPropertyEvent(this, element, hiddenProperty, visibility, data));
        }
    }

    public void markPropertyVisible(
        InMemoryElement element,
        InMemoryTableElement inMemoryTableElement,
        String key,
        String name,
        Visibility propertyVisibility,
        Long timestamp,
        Visibility visibility,
        Object data,
        User user
    ) {
        if (!element.canRead(user)) {
            return;
        }

        Property property = inMemoryTableElement.appendMarkPropertyVisibleMutation(
            key,
            name,
            propertyVisibility,
            timestamp,
            visibility,
            data,
            user
        );

        getSearchIndex().markPropertyVisible(this, element, property, visibility, user);

        if (property != null && hasEventListeners()) {
            fireGraphEvent(new MarkVisiblePropertyEvent(this, element, property, visibility, data));
        }
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, final Long timestamp, Visibility visibility) {
        checkNotNull(outVertexId, "outVertexId cannot be null");
        checkNotNull(inVertexId, "inVertexId cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        return new EdgeBuilderByVertexId(edgeId, outVertexId, inVertexId, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                addValidAuthorizations(authorizations.getAuthorizations());
                return savePreparedEdge(this, getVertexId(Direction.OUT), getVertexId(Direction.IN), timestamp, authorizations.getUser());
            }

            @Override
            public String save(User user) {
                addValidAuthorizations(user.getAuthorizations());
                Edge e = savePreparedEdge(this, getVertexId(Direction.OUT), getVertexId(Direction.IN), timestamp, user);
                return e.getId();
            }
        };
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, final Long timestamp, Visibility visibility) {
        checkNotNull(outVertex, "outVertex cannot be null");
        checkNotNull(inVertex, "inVertex cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                addValidAuthorizations(authorizations.getAuthorizations());
                return savePreparedEdge(this, getOutVertex().getId(), getInVertex().getId(), timestamp, authorizations.getUser());
            }

            @Override
            public String save(User user) {
                addValidAuthorizations(user.getAuthorizations());
                Edge e = savePreparedEdge(this, getOutVertex().getId(), getInVertex().getId(), timestamp, user);
                return e.getId();
            }
        };
    }

    private Edge savePreparedEdge(final EdgeBuilderBase edgeBuilder, final String outVertexId, final String inVertexId, Long timestamp, User user) {
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();

            // The timestamps will be incremented below, this will ensure future mutations will be in the future
            IncreasingTime.advanceTime(10);
        }
        long incrementingTimestamp = timestamp;
        InMemoryTableElement edgeTableElement = this.edges.getTableElement(edgeBuilder.getId());
        boolean isNew = false;
        if (edgeTableElement == null) {
            isNew = true;
            edges.append(
                edgeBuilder.getId(),
                new AlterVisibilityMutation(incrementingTimestamp++, edgeBuilder.getVisibility(), null),
                new ElementTimestampMutation(incrementingTimestamp++),
                new AlterEdgeLabelMutation(incrementingTimestamp++, edgeBuilder.getEdgeLabel()),
                new EdgeSetupMutation(incrementingTimestamp++, outVertexId, inVertexId)
            );
        } else {
            edges.append(edgeBuilder.getId(), new ElementTimestampMutation(incrementingTimestamp++));
            if (edgeBuilder.getNewEdgeLabel() == null) {
                AlterEdgeLabelMutation alterEdgeLabelMutation = (AlterEdgeLabelMutation) edgeTableElement.findLastMutation(AlterEdgeLabelMutation.class);
                if (alterEdgeLabelMutation != null && !alterEdgeLabelMutation.getNewEdgeLabel().equals(edgeBuilder.getEdgeLabel())) {
                    edges.append(edgeBuilder.getId(), new AlterEdgeLabelMutation(incrementingTimestamp++, edgeBuilder.getEdgeLabel()));
                }
            }
        }
        if (edgeBuilder.getNewEdgeLabel() != null) {
            edges.append(edgeBuilder.getId(), new AlterEdgeLabelMutation(incrementingTimestamp, edgeBuilder.getNewEdgeLabel()));
        }

        InMemoryEdge edge = this.edges.get(InMemoryGraph.this, edgeBuilder.getId(), FetchHints.ALL_INCLUDING_HIDDEN, user);
        if (isNew && hasEventListeners()) {
            fireGraphEvent(new AddEdgeEvent(InMemoryGraph.this, edge));
        }
        edge.updatePropertiesInternal(edgeBuilder);

        if (edgeBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
            updateElementAndExtendedDataInSearchIndex(edge, edgeBuilder, user);
        }

        return edge;
    }

    @Override
    public Stream<Edge> getEdges(FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        return this.edges.getAll(InMemoryGraph.this, fetchHints, endTime, user)
            .map(e -> e);
    }

    @Override
    protected GraphMetadataStore getGraphMetadataStore() {
        return graphMetadataStore;
    }

    @Override
    public void deleteEdge(Edge edge, Authorizations authorizations) {
        checkNotNull(edge, "Edge cannot be null");
        if (!((InMemoryEdge) edge).canRead(authorizations)) {
            return;
        }

        deleteAllExtendedDataForElement(edge, authorizations);

        this.edges.remove(edge.getId());
        getSearchIndex().deleteElement(this, edge, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new DeleteEdgeEvent(this, edge));
        }
    }

    @Override
    public void softDeleteEdge(Edge edge, Object eventData, Authorizations authorizations) {

    }

    @Override
    public void softDeleteEdge(Edge edge, Long timestamp, Object eventData, Authorizations authorizations) {
        checkNotNull(edge, "Edge cannot be null");
        if (!((InMemoryEdge) edge).canRead(authorizations)) {
            return;
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        this.edges.getTableElement(edge.getId()).appendSoftDeleteMutation(timestamp, eventData);

        getSearchIndex().deleteElement(this, edge, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new SoftDeleteEdgeEvent(this, edge, eventData));
        }
    }

    @Override
    public GraphQuery query(String queryString, User user) {
        return null;
    }

    @Override
    public MultiVertexQuery query(String[] vertexIds, String queryString, User user) {
        return null;
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(String[] fields, String text, User user) {
        return null;
    }

    @Override
    public void markEdgeHidden(Edge edge, Visibility visibility, Object eventData, Authorizations authorizations) {
        if (!((InMemoryEdge) edge).canRead(authorizations)) {
            return;
        }

        Vertex inVertex = getVertex(edge.getVertexId(Direction.IN), authorizations);
        checkNotNull(inVertex, "Could not find in vertex \"" + edge.getVertexId(Direction.IN) + "\" on edge \"" + edge.getId() + "\"");
        Vertex outVertex = getVertex(edge.getVertexId(Direction.OUT), authorizations);
        checkNotNull(outVertex, "Could not find out vertex \"" + edge.getVertexId(Direction.OUT) + "\" on edge \"" + edge.getId() + "\"");

        this.edges.getTableElement(edge.getId()).appendMarkHiddenMutation(visibility, eventData);
        getSearchIndex().markElementHidden(this, edge, visibility, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new MarkHiddenEdgeEvent(this, edge, eventData));
        }
    }

    @Override
    public void markEdgeVisible(Edge edge, Visibility visibility, Object eventData, Authorizations authorizations) {
        if (!((InMemoryEdge) edge).canRead(authorizations)) {
            return;
        }

        Vertex inVertex = getVertex(edge.getVertexId(Direction.IN), FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
        checkNotNull(inVertex, "Could not find in vertex \"" + edge.getVertexId(Direction.IN) + "\" on edge \"" + edge.getId() + "\"");
        Vertex outVertex = getVertex(edge.getVertexId(Direction.OUT), FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
        checkNotNull(outVertex, "Could not find out vertex \"" + edge.getVertexId(Direction.OUT) + "\" on edge \"" + edge.getId() + "\"");

        this.edges.getTableElement(edge.getId()).appendMarkVisibleMutation(visibility, eventData);
        getSearchIndex().markElementVisible(this, edge, visibility, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new MarkVisibleEdgeEvent(this, edge, eventData));
        }
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        addValidAuthorizations(auths);
        return new InMemoryAuthorizations(auths);
    }

    @Override
    public Stream<String> saveElementMutations(Iterable<ElementMutation<? extends Element>> mutations, User user) {
        return null;
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> ids, FetchHints fetchHints, User user) {
        return null;
    }

    @Override
    public ExtendedDataRow getExtendedData(ExtendedDataRowId id, User user) {
        return null;
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedData(ElementType elementType, String elementId, String tableName, FetchHints fetchHints, User user) {
        return null;
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, Range elementIdRange, User user) {
        return null;
    }

    @Override
    public Stream<HistoricalEvent> getHistoricalEvents(Iterable<ElementId> elementIds, HistoricalEventId after, HistoricalEventsFetchHints fetchHints, User user) {
        return null;
    }

    private Stream<InMemoryTableEdge> getInMemoryTableEdges() {
        return edges.getAllTableElements();
    }

    private Stream<InMemoryTableEdge> getInMemoryTableEdgesForVertex(String vertexId, FetchHints fetchHints, User user) {
        return getInMemoryTableEdges()
            .filter(inMemoryTableElement -> {
                EdgeSetupMutation edgeSetupMutation = inMemoryTableElement.findLastMutation(EdgeSetupMutation.class);
                String inVertexId = edgeSetupMutation.getInVertexId();
                checkNotNull(inVertexId, "inVertexId was null");
                String outVertexId = edgeSetupMutation.getOutVertexId();
                checkNotNull(outVertexId, "outVertexId was null");

                return (inVertexId.equals(vertexId) || outVertexId.equals(vertexId)) &&
                    InMemoryGraph.this.isIncluded(inMemoryTableElement, fetchHints, user);
            });
    }

    protected Stream<Edge> getEdgesFromVertex(
        String vertexId,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        return getInMemoryTableEdgesForVertex(vertexId, fetchHints, user)
            .map(inMemoryTableElement -> (Edge) inMemoryTableElement.createElement(InMemoryGraph.this, fetchHints, endTime, user))
            .filter(Objects::nonNull); // edge deleted or outside of time range
    }

    protected boolean isIncluded(
        InMemoryTableElement element,
        FetchHints fetchHints,
        User user
    ) {
        boolean includeHidden = fetchHints.isIncludeHidden();

        if (!element.canRead(fetchHints, user)) {
            return false;
        }

        if (!includeHidden) {
            if (element.isHidden(user)) {
                return false;
            }
        }

        return true;
    }

    protected boolean isIncludedInTimeSpan(
        InMemoryTableElement element,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        boolean includeHidden = fetchHints.isIncludeHidden();

        if (!element.canRead(fetchHints, user)) {
            return false;
        }
        if (!includeHidden && element.isHidden(user)) {
            return false;
        }

        if (element.isDeleted(endTime, user)) {
            return false;
        }

        if (endTime != null && element.getFirstTimestamp() > endTime) {
            return false;
        }

        return true;
    }

    protected void addAdditionalVisibility(
        InMemoryTableElement inMemoryTableElement,
        String visibility,
        Object eventData,
        User user
    ) {
        Element element;
        FetchHints fetchHints = new FetchHintsBuilder(FetchHints.ALL_INCLUDING_HIDDEN)
            .setIgnoreAdditionalVisibilities(true)
            .build();
        inMemoryTableElement.appendAddAdditionalVisibilityMutation(visibility, eventData);
        if (inMemoryTableElement instanceof InMemoryTableVertex) {
            element = getVertex(inMemoryTableElement.getId(), fetchHints, user);
        } else if (inMemoryTableElement instanceof InMemoryTableEdge) {
            element = getEdge(inMemoryTableElement.getId(), fetchHints, user);
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + inMemoryTableElement.getClass().getName());
        }
        if (hasEventListeners()) {
            fireGraphEvent(new AddAdditionalVisibilityEvent(this, element, visibility, eventData));
        }
    }

    protected void deleteAdditionalVisibility(
        InMemoryTableElement inMemoryTableElement,
        String visibility,
        Object eventData,
        User user
    ) {
        Element element;
        FetchHints fetchHints = new FetchHintsBuilder(FetchHints.ALL_INCLUDING_HIDDEN)
            .setIgnoreAdditionalVisibilities(true)
            .build();
        inMemoryTableElement.appendDeleteAdditionalVisibilityMutation(visibility, eventData);
        if (inMemoryTableElement instanceof InMemoryTableVertex) {
            element = getVertex(inMemoryTableElement.getId(), fetchHints, user);
        } else if (inMemoryTableElement instanceof InMemoryTableEdge) {
            element = getEdge(inMemoryTableElement.getId(), fetchHints, user);
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + inMemoryTableElement.getClass().getName());
        }
        if (hasEventListeners()) {
            fireGraphEvent(new DeleteAdditionalVisibilityEvent(this, element, visibility, eventData));
        }
    }

    protected void softDeleteProperty(
        InMemoryTableElement inMemoryTableElement,
        Property property,
        Long timestamp,
        Object data,
        IndexHint indexHint,
        User user
    ) {
        Element element;
        if (inMemoryTableElement instanceof InMemoryTableVertex) {
            inMemoryTableElement.appendSoftDeletePropertyMutation(property.getKey(), property.getName(), property.getVisibility(), timestamp, data);
            element = getVertex(inMemoryTableElement.getId(), FetchHints.ALL_INCLUDING_HIDDEN, user);
        } else if (inMemoryTableElement instanceof InMemoryTableEdge) {
            inMemoryTableElement.appendSoftDeletePropertyMutation(property.getKey(), property.getName(), property.getVisibility(), timestamp, data);
            element = getEdge(inMemoryTableElement.getId(), FetchHints.ALL_INCLUDING_HIDDEN, user);
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + inMemoryTableElement.getClass().getName());
        }
        if (indexHint != IndexHint.DO_NOT_INDEX) {
            getSearchIndex().deleteProperty(this, element, PropertyDescriptor.fromProperty(property), user);
        }

        if (hasEventListeners()) {
            fireGraphEvent(new SoftDeletePropertyEvent(this, element, property, data));
        }
    }

    public void addPropertyValue(
        InMemoryElement element,
        InMemoryTableElement inMemoryTableElement,
        String key,
        String name,
        Object value,
        Metadata metadata,
        Visibility visibility,
        Long timestamp,
        User user
    ) {
        ensurePropertyDefined(name, value);

        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        if (value instanceof StreamingPropertyValue) {
            value = saveStreamingPropertyValue((StreamingPropertyValue) value);
        }
        inMemoryTableElement.appendAddPropertyValueMutation(key, name, value, metadata, visibility, timestamp, null);
        Property property = inMemoryTableElement.getProperty(key, name, visibility, FetchHints.ALL_INCLUDING_HIDDEN, user);

        if (hasEventListeners()) {
            fireGraphEvent(new AddPropertyEvent(this, element, property));
        }
    }

    protected void alterElementVisibility(InMemoryTableElement inMemoryTableElement, Visibility newEdgeVisibility, Object data) {
        inMemoryTableElement.appendAlterVisibilityMutation(newEdgeVisibility, data);
    }

    protected void alterElementPropertyVisibilities(
        InMemoryTableElement inMemoryTableElement,
        List<AlterPropertyVisibility> alterPropertyVisibilities,
        User user
    ) {
        for (AlterPropertyVisibility apv : alterPropertyVisibilities) {
            Property property = inMemoryTableElement.getProperty(
                apv.getKey(),
                apv.getName(),
                apv.getExistingVisibility(),
                FetchHints.ALL_INCLUDING_HIDDEN,
                user
            );
            if (property == null) {
                throw new VertexiumException("Could not find property " + apv.getKey() + ":" + apv.getName());
            }
            if (apv.getExistingVisibility() == null) {
                apv.setExistingVisibility(property.getVisibility());
            }
            Object value = property.getValue();
            Metadata metadata = property.getMetadata();

            inMemoryTableElement.appendSoftDeletePropertyMutation(
                apv.getKey(),
                apv.getName(),
                apv.getExistingVisibility(),
                apv.getTimestamp(),
                apv.getData()
            );

            long newTimestamp = apv.getTimestamp() + 1;
            if (value instanceof StreamingPropertyValue) {
                value = saveStreamingPropertyValue((StreamingPropertyValue) value);
            }
            inMemoryTableElement.appendAddPropertyValueMutation(
                apv.getKey(),
                apv.getName(),
                value,
                metadata,
                apv.getVisibility(),
                newTimestamp,
                apv.getData()
            );
        }
    }

    protected void alterElementPropertyMetadata(
        InMemoryTableElement inMemoryTableElement,
        List<SetPropertyMetadata> setPropertyMetadatas,
        User user
    ) {
        for (SetPropertyMetadata spm : setPropertyMetadatas) {
            Property property = inMemoryTableElement.getProperty(
                spm.getPropertyKey(),
                spm.getPropertyName(),
                spm.getPropertyVisibility(),
                FetchHints.ALL_INCLUDING_HIDDEN,
                user
            );
            if (property == null) {
                throw new VertexiumException("Could not find property " + spm.getPropertyKey() + ":" + spm.getPropertyName());
            }

            Metadata metadata = Metadata.create(property.getMetadata());
            metadata.add(spm.getMetadataName(), spm.getNewValue(), spm.getMetadataVisibility());

            long newTimestamp = IncreasingTime.currentTimeMillis();
            inMemoryTableElement.appendAddPropertyMetadataMutation(
                property.getKey(), property.getName(), metadata, property.getVisibility(), newTimestamp);
        }
    }

    protected StreamingPropertyValueRef saveStreamingPropertyValue(StreamingPropertyValue value) {
        return new InMemoryStreamingPropertyValueRef(value);
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        return authorizations.canRead(visibility);
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, User user) {
        return false;
    }

    @Override
    public void truncate() {
        this.vertices.clear();
        this.edges.clear();
        getSearchIndex().truncate(this);
    }

    @Override
    public void drop() {
        this.vertices.clear();
        this.edges.clear();
        getSearchIndex().drop(this);
    }

    protected void alterEdgeLabel(InMemoryTableEdge inMemoryTableEdge, long timestamp, String newEdgeLabel) {
        inMemoryTableEdge.appendAlterEdgeLabelMutation(timestamp, newEdgeLabel);
    }

    protected void deleteProperty(
        InMemoryElement element,
        InMemoryTableElement inMemoryTableElement,
        String key,
        String name,
        Visibility visibility,
        User user
    ) {
        Property property = inMemoryTableElement.getProperty(key, name, visibility, FetchHints.ALL_INCLUDING_HIDDEN, user);
        inMemoryTableElement.deleteProperty(key, name, visibility, user);

        getSearchIndex().deleteProperty(this, element, PropertyDescriptor.fromProperty(property), user);

        if (hasEventListeners()) {
            fireGraphEvent(new DeletePropertyEvent(this, element, property));
        }
    }

    private void refreshVertexInMemoryTableElement(Vertex vertex) {
        ((InMemoryVertex) vertex).setInMemoryTableElement(this.vertices.getTableElement(vertex.getId()));
    }

    public ImmutableSet<String> getExtendedDataTableNames(
        ElementType elementType,
        String elementId,
        FetchHints fetchHints,
        User user
    ) {
        return extendedDataTable.getTableNames(elementType, elementId, fetchHints, user);
    }

    public Iterable<? extends ExtendedDataRow> getExtendedDataTable(
        ElementType elementType,
        String elementId,
        String tableName,
        FetchHints fetchHints,
        User user
    ) {
        return extendedDataTable.getTable(elementType, elementId, tableName, fetchHints, user);
    }

    public void extendedData(
        Element element,
        ExtendedDataRowId rowId,
        ExtendedDataMutation extendedData,
        User user
    ) {
        extendedDataTable.addData(rowId, extendedData.getColumnName(), extendedData.getKey(), extendedData.getValue(), extendedData.getTimestamp(), extendedData.getVisibility());
        getSearchIndex().addElementExtendedData(
            this,
            element,
            Collections.singleton(extendedData),
            Collections.emptyList(),
            Collections.emptyList(),
            user
        );
        if (hasEventListeners()) {
            fireGraphEvent(new AddExtendedDataEvent(
                this,
                element,
                rowId.getTableName(),
                rowId.getRowId(),
                extendedData.getColumnName(),
                extendedData.getKey(),
                extendedData.getValue(),
                extendedData.getVisibility()
            ));
        }
    }

    @Override
    public void deleteExtendedDataRow(ExtendedDataRowId id, Authorizations authorizations) {
        List<ExtendedDataRow> rows = Lists.newArrayList(getExtendedData(Lists.newArrayList(id), authorizations));
        if (rows.size() > 1) {
            throw new VertexiumException("Found too many extended data rows for id: " + id);
        }
        if (rows.size() != 1) {
            return;
        }

        this.extendedDataTable.remove(id);
        getSearchIndex().deleteExtendedData(this, id, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new DeleteExtendedDataRowEvent(this, id));
        }
    }

    public void deleteExtendedData(
        InMemoryElement element,
        String tableName,
        String row,
        String columnName,
        String key,
        Visibility visibility,
        User user
    ) {
        extendedDataTable.removeColumn(
            new ExtendedDataRowId(ElementType.getTypeFromElement(element), element.getId(), tableName, row),
            columnName,
            key,
            visibility
        );

        getSearchIndex().deleteExtendedData(this, element, tableName, row, columnName, key, visibility, user);
        if (hasEventListeners()) {
            fireGraphEvent(new DeleteExtendedDataEvent(this, element, tableName, row, columnName, key));
        }
    }

    public void addAdditionalExtendedDataVisibility(
        InMemoryElement element,
        String tableName,
        String row,
        String additionalVisibility
    ) {
        extendedDataTable.addAdditionalVisibility(
            new ExtendedDataRowId(ElementType.getTypeFromElement(element), element.getId(), tableName, row),
            additionalVisibility
        );

        if (hasEventListeners()) {
            fireGraphEvent(new AddAdditionalExtendedDataVisibilityEvent(this, element, tableName, row, additionalVisibility));
        }
    }

    public void deleteAdditionalExtendedDataVisibility(
        InMemoryElement element,
        String tableName,
        String row,
        String additionalVisibility
    ) {
        extendedDataTable.deleteAdditionalVisibility(
            new ExtendedDataRowId(ElementType.getTypeFromElement(element), element.getId(), tableName, row),
            additionalVisibility
        );

        if (hasEventListeners()) {
            fireGraphEvent(new DeleteAdditionalExtendedDataVisibilityEvent(this, element, tableName, row, additionalVisibility));
        }
    }

    @Override
    public void flushGraph() {
        // no need to do anything here
    }

    Stream<HistoricalEvent> getHistoricalVertexEdgeEvents(
        String vertexId,
        HistoricalEventsFetchHints historicalEventsFetchHints,
        User user
    ) {
        FetchHints elementFetchHints = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeHidden(true)
            .setIncludeAllEdgeRefs(true)
            .build();
        return getInMemoryTableEdgesForVertex(vertexId, elementFetchHints, user)
            .flatMap(inMemoryTableElement -> inMemoryTableElement.getHistoricalEventsForVertex(vertexId, historicalEventsFetchHints));
    }

    @Override
    public Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, User user) {
        return getVertices(fetchHints, endTime, user)
            .filter(v -> v.getId().equals(vertexId))
            .findFirst()
            .orElse(null);
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, User user) {
        return getEdges(fetchHints, endTime, user)
            .filter(e -> e.getId().equals(edgeId))
            .findFirst()
            .orElse(null);
    }

    @Override
    public Stream<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Long endTime, User user) {
        return getVertices(fetchHints, endTime, user)
            .filter(v -> v.getId().startsWith(vertexIdPrefix));
    }

    @Override
    public Stream<Vertex> getVerticesInRange(Range idRange, FetchHints fetchHints, Long endTime, User user) {
        return getVertices(fetchHints, endTime, user)
            .filter(v -> idRange.isInRange(v.getId()));
    }

    @Override
    public Stream<Edge> getEdgesInRange(Range idRange, FetchHints fetchHints, Long endTime, User user) {
        return getEdges(fetchHints, endTime, user)
            .filter(e -> idRange.isInRange(e.getId()));
    }

    @Override
    public Stream<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user) {
        return stream(ids)
            .distinct()
            .map(id -> getVertex(id, fetchHints, endTime, user))
            .filter(Objects::nonNull);
    }

    @Override
    public Stream<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user) {
        return stream(ids)
            .distinct()
            .map(id -> getEdge(id, fetchHints, endTime, user))
            .filter(Objects::nonNull);
    }

    @Override
    public Iterable<String> filterEdgeIdsByAuthorization(
        Iterable<String> edgeIds,
        final String authorizationToMatch,
        final EnumSet<ElementFilter> filters,
        Authorizations authorizations
    ) {
        FilterIterable<Edge> edges = new FilterIterable<Edge>(getEdges(edgeIds, FetchHints.ALL_INCLUDING_HIDDEN, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                if (filters.contains(ElementFilter.ELEMENT)) {
                    if (edge.getVisibility().hasAuthorization(authorizationToMatch)) {
                        return true;
                    }
                }
                return isIncludedByAuthorizations(edge, filters, authorizationToMatch);
            }
        };
        return new ConvertingIterable<Edge, String>(edges) {
            @Override
            protected String convert(Edge edge) {
                return edge.getId();
            }
        };
    }

    @Override
    public Iterable<String> filterVertexIdsByAuthorization(
        Iterable<String> vertexIds,
        final String authorizationToMatch,
        final EnumSet<ElementFilter> filters,
        Authorizations authorizations
    ) {
        FilterIterable<Vertex> vertices = new FilterIterable<Vertex>(getVertices(vertexIds, FetchHints.ALL_INCLUDING_HIDDEN, authorizations)) {
            @Override
            protected boolean isIncluded(Vertex vertex) {
                if (filters.contains(ElementFilter.ELEMENT)) {
                    if (vertex.getVisibility().hasAuthorization(authorizationToMatch)) {
                        return true;
                    }
                }
                return isIncludedByAuthorizations(vertex, filters, authorizationToMatch);
            }
        };
        return new ConvertingIterable<Vertex, String>(vertices) {
            @Override
            protected String convert(Vertex vertex) {
                return vertex.getId();
            }
        };
    }

    private boolean isIncludedByAuthorizations(Element element, EnumSet<ElementFilter> filters, String authorizationToMatch) {
        if (filters.contains(ElementFilter.PROPERTY) || filters.contains(ElementFilter.PROPERTY_METADATA)) {
            for (Property property : element.getProperties()) {
                if (filters.contains(ElementFilter.PROPERTY)) {
                    if (property.getVisibility().hasAuthorization(authorizationToMatch)) {
                        return true;
                    }
                }
                if (filters.contains(ElementFilter.PROPERTY_METADATA)) {
                    for (Metadata.Entry entry : property.getMetadata().entrySet()) {
                        if (entry.getVisibility().hasAuthorization(authorizationToMatch)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    @Override
    public Stream<Path> findPaths(FindPathOptions options, User user) {
        ProgressCallback progressCallback = options.getProgressCallback();
        if (progressCallback == null) {
            progressCallback = new ProgressCallback() {
                @Override
                public void progress(double progressPercent, Step step, Integer edgeIndex, Integer vertexCount) {
                    LOGGER.debug("findPaths progress %d%%: %s", (int) (progressPercent * 100.0), step.formatMessage(edgeIndex, vertexCount));
                }
            };
        }

        FetchHints fetchHints = FetchHints.EDGE_REFS;
        Vertex sourceVertex = getVertex(options.getSourceVertexId(), fetchHints, user);
        if (sourceVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + options.getSourceVertexId());
        }
        Vertex destVertex = getVertex(options.getDestVertexId(), fetchHints, user);
        if (destVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + options.getDestVertexId());
        }

        progressCallback.progress(0, ProgressCallback.Step.FINDING_PATH);

        Set<String> seenVertices = new HashSet<>();
        seenVertices.add(sourceVertex.getId());

        Path startPath = new Path(sourceVertex.getId());

        List<Path> foundPaths = new ArrayList<>();
        if (options.getMaxHops() == 2) {
            findPathsSetIntersection(
                options,
                foundPaths,
                sourceVertex,
                destVertex,
                progressCallback,
                user
            );
        } else {
            findPathsRecursive(
                options,
                foundPaths,
                sourceVertex,
                destVertex,
                options.getMaxHops(),
                seenVertices,
                startPath,
                progressCallback,
                user
            );
        }

        progressCallback.progress(1, ProgressCallback.Step.COMPLETE);
        return foundPaths.stream();
    }

    private void findPathsSetIntersection(
        FindPathOptions options,
        List<Path> foundPaths,
        Vertex sourceVertex,
        Vertex destVertex,
        ProgressCallback progressCallback,
        User user
    ) {
        String sourceVertexId = sourceVertex.getId();
        String destVertexId = destVertex.getId();

        progressCallback.progress(0.1, ProgressCallback.Step.SEARCHING_SOURCE_VERTEX_EDGES);
        Set<String> sourceVertexConnectedVertexIds = filterFindPathEdgeInfo(options, sourceVertex.getEdgeInfos(Direction.BOTH, options.getLabels(), user));
        Map<String, Boolean> sourceVerticesExist = doVerticesExist(sourceVertexConnectedVertexIds, user);
        sourceVertexConnectedVertexIds = stream(sourceVerticesExist.keySet())
            .filter(key -> sourceVerticesExist.getOrDefault(key, false))
            .collect(Collectors.toSet());

        progressCallback.progress(0.3, ProgressCallback.Step.SEARCHING_DESTINATION_VERTEX_EDGES);
        Set<String> destVertexConnectedVertexIds = filterFindPathEdgeInfo(options, destVertex.getEdgeInfos(Direction.BOTH, options.getLabels(), user));
        Map<String, Boolean> destVerticesExist = doVerticesExist(destVertexConnectedVertexIds, user);
        destVertexConnectedVertexIds = stream(destVerticesExist.keySet())
            .filter(key -> destVerticesExist.getOrDefault(key, false))
            .collect(Collectors.toSet());

        if (sourceVertexConnectedVertexIds.contains(destVertexId)) {
            foundPaths.add(new Path(sourceVertexId, destVertexId));
            if (options.isGetAnyPath()) {
                return;
            }
        }

        progressCallback.progress(0.6, ProgressCallback.Step.MERGING_EDGES);
        sourceVertexConnectedVertexIds.retainAll(destVertexConnectedVertexIds);

        progressCallback.progress(0.9, ProgressCallback.Step.ADDING_PATHS);
        for (String connectedVertexId : sourceVertexConnectedVertexIds) {
            foundPaths.add(new Path(sourceVertexId, connectedVertexId, destVertexId));
        }
    }

    private void findPathsRecursive(
        FindPathOptions options,
        List<Path> foundPaths,
        Vertex sourceVertex,
        Vertex destVertex,
        int hops,
        Set<String> seenVertices,
        Path currentPath,
        ProgressCallback progressCallback,
        User user
    ) {
        // if this is our first source vertex report progress back to the progress callback
        boolean firstLevelRecursion = hops == options.getMaxHops();

        if (options.isGetAnyPath() && foundPaths.size() == 1) {
            return;
        }

        seenVertices.add(sourceVertex.getId());
        if (sourceVertex.getId().equals(destVertex.getId())) {
            foundPaths.add(currentPath);
        } else if (hops > 0) {
            Iterable<Vertex> vertices = filterFindPathEdgePairs(options, sourceVertex.getEdgeVertexPairs(Direction.BOTH, options.getLabels(), user));
            int vertexCount = 0;
            if (firstLevelRecursion) {
                vertices = IterableUtils.toList(vertices);
                vertexCount = ((List<Vertex>) vertices).size();
            }
            int i = 0;
            for (Vertex child : vertices) {
                if (firstLevelRecursion) {
                    // this will never get to 100% since i starts at 0. which is good. 100% signifies done and we still have work to do.
                    double progressPercent = (double) i / (double) vertexCount;
                    progressCallback.progress(progressPercent, ProgressCallback.Step.SEARCHING_EDGES, i + 1, vertexCount);
                }
                if (!seenVertices.contains(child.getId())) {
                    findPathsRecursive(options, foundPaths, child, destVertex, hops - 1, seenVertices, new Path(currentPath, child.getId()), progressCallback, user);
                }
                i++;
            }
        }
        seenVertices.remove(sourceVertex.getId());
    }

    private Set<String> filterFindPathEdgeInfo(FindPathOptions options, Stream<EdgeInfo> edgeInfos) {
        return edgeInfos
            .filter(edgeInfo -> {
                if (options.getExcludedLabels() != null) {
                    return !ArrayUtils.contains(options.getExcludedLabels(), edgeInfo.getLabel());
                }
                return true;
            })
            .map(EdgeInfo::getVertexId)
            .collect(Collectors.toSet());
    }

    private Iterable<Vertex> filterFindPathEdgePairs(FindPathOptions options, Stream<EdgeVertexPair> edgeVertexPairs) {
        return edgeVertexPairs
            .filter(edgePair -> {
                if (options.getExcludedLabels() != null) {
                    return !ArrayUtils.contains(options.getExcludedLabels(), edgePair.getEdge().getLabel());
                }
                return true;
            })
            .map(EdgeVertexPair::getVertex)
            .collect(Collectors.toList());
    }

    @Override
    public Stream<String> findRelatedEdgeIds(Iterable<String> vertexIds, Long endTime, User user) {
        FetchHints fetchHints = new FetchHintsBuilder()
            .setIncludeOutEdgeRefs(true)
            .build();
        return findRelatedEdgeIdsForVertices(
            getVertices(vertexIds, fetchHints, endTime, user).collect(Collectors.toList()),
            user
        );
    }

    @Override
    public Stream<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Long endTime, User user) {
        FetchHints fetchHints = new FetchHintsBuilder()
            .setIncludeOutEdgeRefs(true)
            .build();
        return findRelatedEdgeSummaryForVertices(
            getVertices(vertexIds, fetchHints, endTime, user).collect(Collectors.toList()),
            user
        );
    }
}
