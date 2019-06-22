package org.vertexium.inmemory;

import com.google.common.collect.Sets;
import org.vertexium.*;
import org.vertexium.id.IdGenerator;
import org.vertexium.inmemory.mutations.EdgeSetupMutation;
import org.vertexium.mutation.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.search.SearchIndex;
import org.vertexium.util.IncreasingTime;

import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.vertexium.util.StreamUtils.stream;

public class InMemoryGraph extends GraphBase {
    protected static final InMemoryGraphConfiguration DEFAULT_CONFIGURATION =
        new InMemoryGraphConfiguration(new HashMap<>());
    private final Set<String> validAuthorizations = new HashSet<>();
    private final InMemoryVertexTable vertices;
    private final InMemoryEdgeTable edges;
    private final InMemoryExtendedDataTable extendedDataTable;
    private final GraphMetadataStore graphMetadataStore;
    private final InMemoryElementMutationBuilder elementMutationBuilder;
    private final InMemoryFindPathStrategy findPathStrategy = new InMemoryFindPathStrategy(this);

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
        this.elementMutationBuilder = createInMemoryElementMutationBuilder();
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
        this.elementMutationBuilder = createInMemoryElementMutationBuilder();
    }

    private InMemoryElementMutationBuilder createInMemoryElementMutationBuilder() {
        return new InMemoryElementMutationBuilder(
            this,
            this.vertices,
            this.edges,
            this.extendedDataTable
        );
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
        checkNotNull(visibility, "visibility is required");
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        long finalTimestamp = timestamp;

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
                return elementMutationBuilder.saveVertexBuilder(this, finalTimestamp, user);
            }
        };
    }

    <T extends Element> void updateElementAndExtendedDataInSearchIndex(
        Element element,
        ElementMutation<T> elementMutation,
        User user
    ) {
        if (elementMutation instanceof ExistingElementMutation) {
            getSearchIndex().addOrUpdateElement(this, elementMutation, user);
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

    void addValidAuthorizations(String[] authorizations) {
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
    public EdgeBuilderByVertexId prepareEdge(
        String edgeId,
        String outVertexId,
        String inVertexId,
        String label,
        Long timestamp,
        Visibility visibility
    ) {
        checkNotNull(outVertexId, "outVertexId cannot be null");
        checkNotNull(inVertexId, "inVertexId cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();

            // The timestamps will be incremented below, this will ensure future mutations will be in the future
            IncreasingTime.advanceTime(10);
        }
        long finalTimestamp = timestamp;

        return new EdgeBuilderByVertexId(edgeId, outVertexId, inVertexId, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                addValidAuthorizations(authorizations.getAuthorizations());
                return elementMutationBuilder.savePreparedEdge(
                    this,
                    getVertexId(Direction.OUT),
                    getVertexId(Direction.IN),
                    finalTimestamp,
                    authorizations.getUser()
                );
            }

            @Override
            public String save(User user) {
                addValidAuthorizations(user.getAuthorizations());
                Edge e = elementMutationBuilder.savePreparedEdge(
                    this,
                    getVertexId(Direction.OUT),
                    getVertexId(Direction.IN),
                    finalTimestamp,
                    user
                );
                return e.getId();
            }
        };
    }

    @Override
    public EdgeBuilder prepareEdge(
        String edgeId,
        Vertex outVertex,
        Vertex inVertex,
        String label,
        Long timestamp,
        Visibility visibility
    ) {
        checkNotNull(outVertex, "outVertex cannot be null");
        checkNotNull(inVertex, "inVertex cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();

            // The timestamps will be incremented below, this will ensure future mutations will be in the future
            IncreasingTime.advanceTime(10);
        }
        long finalTimestamp = timestamp;

        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                addValidAuthorizations(authorizations.getAuthorizations());
                return elementMutationBuilder.savePreparedEdge(
                    this,
                    getOutVertex().getId(),
                    getInVertex().getId(),
                    finalTimestamp,
                    authorizations.getUser()
                );
            }

            @Override
            public String save(User user) {
                addValidAuthorizations(user.getAuthorizations());
                Edge e = elementMutationBuilder.savePreparedEdge(
                    this,
                    getOutVertex().getId(),
                    getInVertex().getId(),
                    finalTimestamp,
                    user
                );
                return e.getId();
            }
        };
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
    public Authorizations createAuthorizations(String... auths) {
        addValidAuthorizations(auths);
        return new InMemoryAuthorizations(auths);
    }

    private Stream<InMemoryTableEdge> getInMemoryTableEdges() {
        return edges.getAllTableElements();
    }

    Stream<InMemoryTableEdge> getInMemoryTableEdgesForVertex(String vertexId, FetchHints fetchHints, User user) {
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

    @Override
    public boolean isVisibilityValid(Visibility visibility, User user) {
        return user.canRead(visibility);
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

    InMemoryExtendedDataTable getExtendedDataTable() {
        return extendedDataTable;
    }

    @Override
    public void flushGraph() {
        // no need to do anything here
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
    public Stream<Vertex> getVerticesInRange(IdRange idRange, FetchHints fetchHints, Long endTime, User user) {
        return getVertices(fetchHints, endTime, user)
            .filter(v -> idRange.isInRange(v.getId()));
    }

    @Override
    public Stream<Edge> getEdgesInRange(IdRange idRange, FetchHints fetchHints, Long endTime, User user) {
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
    public Stream<Path> findPaths(FindPathOptions options, User user) {
        return findPathStrategy.findPaths(options, user);
    }

    public InMemoryElementMutationBuilder getElementMutationBuilder() {
        return elementMutationBuilder;
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> idsIterable, FetchHints fetchHints, User user) {
        Set<ExtendedDataRowId> ids = Sets.newHashSet(idsIterable);
        return getAllExtendedData(fetchHints, user)
            .filter(row -> ids.contains(row.getId()));
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedData(
        ElementType elementType,
        String elementId,
        String tableName,
        FetchHints fetchHints,
        User user
    ) {
        if ((elementType == null && (elementId != null || tableName != null))
            || (elementType != null && elementId == null && tableName != null)) {
            throw new VertexiumException("Cannot create partial key with missing inner value");
        }

        return getAllExtendedData(fetchHints, user)
            .filter(row -> {
                ExtendedDataRowId rowId = row.getId();
                return (elementType == null || elementType.equals(rowId.getElementType()))
                    && (elementId == null || elementId.equals(rowId.getElementId()))
                    && (tableName == null || tableName.equals(rowId.getTableName()));
            });
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, IdRange elementIdRange, User user) {
        return getAllExtendedData(FetchHints.ALL, user)
            .filter(row -> {
                ExtendedDataRowId rowId = row.getId();
                return elementType.equals(rowId.getElementType())
                    && elementIdRange.isInRange(rowId.getElementId());
            });
    }

    private Stream<ExtendedDataRow> getAllExtendedData(FetchHints fetchHints, User user) {
        return Stream.concat(getVertices(fetchHints, user), getEdges(fetchHints, user))
            .flatMap(element -> element.getExtendedDataTableNames().stream()
                .flatMap(tableName -> stream(element.getExtendedData(tableName))));
    }

    @Override
    public List<InputStream> getStreamingPropertyValueInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        return streamingPropertyValues.stream()
            .map(StreamingPropertyValue::getInputStream)
            .collect(Collectors.toList());
    }
}
