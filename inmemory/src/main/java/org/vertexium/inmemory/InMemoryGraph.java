package org.vertexium.inmemory;

import org.vertexium.*;
import org.vertexium.event.*;
import org.vertexium.id.IdGenerator;
import org.vertexium.mutation.AlterPropertyVisibility;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.mutation.SetPropertyMetadata;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.search.IndexHint;
import org.vertexium.search.SearchIndex;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.JavaSerializableUtils;
import org.vertexium.util.LookAheadIterable;

import java.util.*;

import static org.vertexium.util.Preconditions.checkNotNull;

public class InMemoryGraph extends GraphBaseWithSearchIndex {
    private static final InMemoryGraphConfiguration DEFAULT_CONFIGURATION = new InMemoryGraphConfiguration(new HashMap());
    private final Map<String, InMemoryVertex> vertices;
    private final Map<String, InMemoryVertex> softDeletedVertices;
    private final Map<String, InMemoryEdge> edges;
    private final Map<String, InMemoryEdge> softDeletedEdges;
    private final Map<String, byte[]> metadata = new HashMap<>();

    protected InMemoryGraph(InMemoryGraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        this(
                configuration,
                idGenerator,
                searchIndex,
                new HashMap<String, InMemoryVertex>(),
                new HashMap<String, InMemoryVertex>(),
                new HashMap<String, InMemoryEdge>(),
                new HashMap<String, InMemoryEdge>()
        );
    }

    protected InMemoryGraph(
            InMemoryGraphConfiguration configuration,
            IdGenerator idGenerator,
            SearchIndex searchIndex,
            Map<String, InMemoryVertex> vertices,
            Map<String, InMemoryVertex> softDeletedVertices,
            Map<String, InMemoryEdge> edges,
            Map<String, InMemoryEdge> softDeletedEdges
    ) {
        super(configuration, idGenerator, searchIndex);
        this.vertices = vertices;
        this.softDeletedVertices = softDeletedVertices;
        this.edges = edges;
        this.softDeletedEdges = softDeletedEdges;
    }

    @SuppressWarnings("unused")
    public static InMemoryGraph create() {
        return create(DEFAULT_CONFIGURATION);
    }

    public static InMemoryGraph create(InMemoryGraphConfiguration config) {
        IdGenerator idGenerator = config.createIdGenerator();
        SearchIndex searchIndex = config.createSearchIndex();
        return create(config, idGenerator, searchIndex);
    }

    public static InMemoryGraph create(InMemoryGraphConfiguration config, IdGenerator idGenerator, SearchIndex searchIndex) {
        InMemoryGraph graph = new InMemoryGraph(config, idGenerator, searchIndex);
        graph.setup();
        return graph;
    }

    @SuppressWarnings("unused")
    public static InMemoryGraph create(Map config) {
        return create(new InMemoryGraphConfiguration(config));
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = System.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        return new VertexBuilder(vertexId, visibility) {
            @Override
            public Vertex save(Authorizations authorizations) {
                InMemoryVertex existingVertex = (InMemoryVertex) getVertex(getVertexId(), authorizations);
                long startTime = existingVertex == null ? timestampLong : existingVertex.getStartTime();

                Iterable<Visibility> hiddenVisibilities = null;
                InMemoryVertex newVertex = new InMemoryVertex(
                        InMemoryGraph.this,
                        getVertexId(),
                        getVisibility(),
                        getProperties(),
                        new InMemoryHistoricalPropertyValues(),
                        getPropertyDeletes(),
                        getPropertySoftDeletes(),
                        hiddenVisibilities,
                        startTime,
                        timestampLong,
                        authorizations
                );

                // to more closely simulate how accumulo works. add a potentially sparse (in case of an update) vertex to the search index.
                if (getIndexHint() != IndexHint.DO_NOT_INDEX) {
                    getSearchIndex().addElement(InMemoryGraph.this, newVertex, authorizations);
                }

                InMemoryVertex vertex = InMemoryVertex.updateOrCreate(InMemoryGraph.this, existingVertex, newVertex, authorizations);
                vertices.put(getVertexId(), vertex);

                if (hasEventListeners()) {
                    fireGraphEvent(new AddVertexEvent(InMemoryGraph.this, vertex));
                    for (Property property : getProperties()) {
                        fireGraphEvent(new AddPropertyEvent(InMemoryGraph.this, vertex, property));
                    }
                    for (PropertyDeleteMutation propertyDeleteMutation : getPropertyDeletes()) {
                        fireGraphEvent(new DeletePropertyEvent(InMemoryGraph.this, vertex, propertyDeleteMutation));
                    }
                }

                return vertex;
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(EnumSet<FetchHint> fetchHints, final Long endTime, final Authorizations authorizations) throws VertexiumException {
        final boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);

        return new LookAheadIterable<InMemoryVertex, Vertex>() {
            @Override
            protected boolean isIncluded(InMemoryVertex src, Vertex vertex) {
                return InMemoryGraph.this.isIncluded(src, includeHidden, endTime, authorizations);
            }

            @Override
            protected Vertex convert(InMemoryVertex vertex) {
                return filteredVertex(vertex, includeHidden, endTime, authorizations);
            }

            @Override
            protected Iterator<InMemoryVertex> createIterator() {
                return vertices.values().iterator();
            }
        };
    }

    private boolean isIncluded(InMemoryElement src, boolean includeHidden, Long endTime, Authorizations authorizations) {
        if (!src.canRead(authorizations)) {
            return false;
        }

        if (!includeHidden && src.isHidden(authorizations)) {
            return false;
        }

        if (endTime != null && src.getStartTime() > endTime) {
            return false;
        }

        return true;
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

        this.vertices.remove(vertex.getId());
        getSearchIndex().deleteElement(this, vertex, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new DeleteVertexEvent(this, vertex));
        }
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Authorizations authorizations) {
        if (!((InMemoryVertex) vertex).canRead(authorizations)) {
            return;
        }

        List<Edge> edgesToSoftDelete = IterableUtils.toList(vertex.getEdges(Direction.BOTH, authorizations));
        for (Edge edgeToSoftDelete : edgesToSoftDelete) {
            softDeleteEdge(edgeToSoftDelete, authorizations);
        }

        this.vertices.remove(vertex.getId());
        this.softDeletedVertices.put(vertex.getId(), (InMemoryVertex) vertex);

        getSearchIndex().deleteElement(this, vertex, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new SoftDeleteVertexEvent(this, vertex));
        }
    }

    @Override
    public void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        if (!((InMemoryVertex) vertex).canRead(authorizations)) {
            return;
        }

        List<Edge> edgesToMarkHidden = IterableUtils.toList(vertex.getEdges(Direction.BOTH, authorizations));
        for (Edge edgeToMarkHidden : edgesToMarkHidden) {
            markEdgeHidden(edgeToMarkHidden, visibility, authorizations);
        }

        this.vertices.get(vertex.getId()).addHiddenVisibility(visibility);
        getSearchIndex().addElement(this, vertex, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new MarkHiddenVertexEvent(this, vertex));
        }
    }

    @Override
    public void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        if (!((InMemoryVertex) vertex).canRead(authorizations)) {
            return;
        }

        List<Edge> edgesToMarkVisible = IterableUtils.toList(vertex.getEdges(Direction.BOTH, FetchHint.ALL_INCLUDING_HIDDEN, authorizations));
        for (Edge edgeToMarkVisible : edgesToMarkVisible) {
            markEdgeVisible(edgeToMarkVisible, visibility, authorizations);
        }

        this.vertices.get(vertex.getId()).removeHiddenVisibility(visibility);
        getSearchIndex().addElement(this, vertex, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new MarkVisibleVertexEvent(this, vertex));
        }
    }

    public void markPropertyHidden(InMemoryElement element, Property property, Visibility visibility, Authorizations authorizations) {
        if (!element.canRead(authorizations)) {
            return;
        }

        if (element instanceof Vertex) {
            this.vertices.get(element.getId()).markPropertyHiddenInternal(property, visibility);
        } else if (element instanceof Edge) {
            this.edges.get(element.getId()).markPropertyHiddenInternal(property, visibility);
        }

        if (hasEventListeners()) {
            fireGraphEvent(new MarkHiddenPropertyEvent(this, element, property, visibility));
        }
    }

    public void markPropertyVisible(InMemoryElement element, Property property, Visibility visibility, Authorizations authorizations) {
        if (!element.canRead(authorizations)) {
            return;
        }

        if (element instanceof Vertex) {
            this.vertices.get(element.getId()).markPropertyVisibleInternal(property, visibility);
        } else if (element instanceof Edge) {
            this.edges.get(element.getId()).markPropertyVisibleInternal(property, visibility);
        }

        if (hasEventListeners()) {
            fireGraphEvent(new MarkVisiblePropertyEvent(this, element, property, visibility));
        }
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, final Long timestamp, Visibility visibility) {
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        return new EdgeBuilderByVertexId(edgeId, outVertexId, inVertexId, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                return savePreparedEdge(this, getOutVertexId(), getInVertexId(), timestamp, authorizations);
            }
        };
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, final Long timestamp, Visibility visibility) {
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                return savePreparedEdge(this, getOutVertex().getId(), getInVertex().getId(), timestamp, authorizations);
            }
        };
    }

    private Edge savePreparedEdge(EdgeBuilderBase edgeBuilder, String outVertexId, String inVertexId, Long timestamp, Authorizations authorizations) {
        Edge existingEdge = getEdge(edgeBuilder.getEdgeId(), authorizations);
        long startTime;

        if (timestamp == null) {
            timestamp = System.currentTimeMillis();
        }

        Iterable<Property> properties;
        if (existingEdge == null) {
            properties = edgeBuilder.getProperties();
        } else {
            Iterable<Property> existingProperties = existingEdge.getProperties();
            Iterable<Property> newProperties = edgeBuilder.getProperties();
            properties = new TreeSet<>(IterableUtils.toList(existingProperties));
            for (Property p : newProperties) {
                ((TreeSet<Property>) properties).remove(p);
                ((TreeSet<Property>) properties).add(p);
            }
        }

        Iterable<Visibility> hiddenVisibilities = null;
        if (existingEdge == null) {
            startTime = timestamp;
        } else if (existingEdge instanceof InMemoryEdge) {
            hiddenVisibilities = ((InMemoryEdge) existingEdge).getHiddenVisibilities();
            startTime = ((InMemoryEdge) existingEdge).getStartTime();
        } else {
            startTime = timestamp;
        }

        String edgeLabel = edgeBuilder.getNewEdgeLabel();
        if (edgeLabel == null) {
            edgeLabel = edgeBuilder.getLabel();
        }

        InMemoryEdge edge = new InMemoryEdge(
                InMemoryGraph.this,
                edgeBuilder.getEdgeId(),
                outVertexId,
                inVertexId,
                edgeLabel,
                edgeBuilder.getVisibility(),
                properties,
                new InMemoryHistoricalPropertyValues(),
                edgeBuilder.getPropertyDeletes(),
                edgeBuilder.getPropertySoftDeletes(),
                hiddenVisibilities,
                startTime,
                timestamp,
                authorizations
        );
        edges.put(edgeBuilder.getEdgeId(), edge);

        if (edgeBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
            getSearchIndex().addElement(InMemoryGraph.this, edge, authorizations);
        }

        if (hasEventListeners()) {
            fireGraphEvent(new AddEdgeEvent(InMemoryGraph.this, edge));
            for (Property property : edgeBuilder.getProperties()) {
                fireGraphEvent(new AddPropertyEvent(InMemoryGraph.this, edge, property));
            }
            for (PropertyDeleteMutation propertyDeleteMutation : edgeBuilder.getPropertyDeletes()) {
                fireGraphEvent(new DeletePropertyEvent(InMemoryGraph.this, edge, propertyDeleteMutation));
            }
        }

        return edge;
    }

    @Override
    public Iterable<Edge> getEdges(EnumSet<FetchHint> fetchHints, final Long endTime, final Authorizations authorizations) {
        final boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);

        return new LookAheadIterable<InMemoryEdge, Edge>() {
            @Override
            protected boolean isIncluded(InMemoryEdge src, Edge edge) {
                return InMemoryGraph.this.isIncluded(src, includeHidden, endTime, authorizations);
            }

            @Override
            protected Edge convert(InMemoryEdge edge) {
                return filteredEdge(edge, includeHidden, endTime, authorizations);
            }

            @Override
            protected Iterator<InMemoryEdge> createIterator() {
                return edges.values().iterator();
            }
        };
    }

    @Override
    public void deleteEdge(Edge edge, Authorizations authorizations) {
        checkNotNull(edge, "Edge cannot be null");
        if (!((InMemoryEdge) edge).canRead(authorizations)) {
            return;
        }

        this.edges.remove(edge.getId());
        getSearchIndex().deleteElement(this, edge, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new DeleteEdgeEvent(this, edge));
        }
    }

    @Override
    public void softDeleteEdge(Edge edge, Authorizations authorizations) {
        checkNotNull(edge, "Edge cannot be null");
        if (!((InMemoryEdge) edge).canRead(authorizations)) {
            return;
        }

        this.edges.remove(edge.getId());
        this.softDeletedEdges.put(edge.getId(), (InMemoryEdge) edge);

        getSearchIndex().deleteElement(this, edge, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new SoftDeleteEdgeEvent(this, edge));
        }
    }

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        return new ConvertingIterable<Map.Entry<String, byte[]>, GraphMetadataEntry>(this.metadata.entrySet()) {
            @Override
            protected GraphMetadataEntry convert(Map.Entry<String, byte[]> o) {
                return new GraphMetadataEntry(o.getKey(), o.getValue());
            }
        };
    }

    @Override
    public Object getMetadata(String key) {
        return JavaSerializableUtils.bytesToObject(this.metadata.get(key));
    }

    @Override
    public void setMetadata(String key, Object value) {
        this.metadata.put(key, JavaSerializableUtils.objectToBytes(value));
    }

    @Override
    public void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations) {
        if (!((InMemoryEdge) edge).canRead(authorizations)) {
            return;
        }

        Vertex inVertex = getVertex(edge.getVertexId(Direction.IN), authorizations);
        checkNotNull(inVertex, "Could not find in vertex: " + edge.getVertexId(Direction.IN));
        Vertex outVertex = getVertex(edge.getVertexId(Direction.OUT), authorizations);
        checkNotNull(outVertex, "Could not find out vertex: " + edge.getVertexId(Direction.OUT));

        this.edges.get(edge.getId()).addHiddenVisibility(visibility);
        getSearchIndex().addElement(this, edge, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new MarkHiddenEdgeEvent(this, edge));
        }
    }

    @Override
    public void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations) {
        if (!((InMemoryEdge) edge).canRead(authorizations)) {
            return;
        }

        Vertex inVertex = getVertex(edge.getVertexId(Direction.IN), FetchHint.ALL_INCLUDING_HIDDEN, authorizations);
        checkNotNull(inVertex, "Could not find in vertex: " + edge.getVertexId(Direction.IN));
        Vertex outVertex = getVertex(edge.getVertexId(Direction.OUT), FetchHint.ALL_INCLUDING_HIDDEN, authorizations);
        checkNotNull(outVertex, "Could not find out vertex: " + edge.getVertexId(Direction.OUT));

        this.edges.get(edge.getId()).removeHiddenVisibility(visibility);
        getSearchIndex().addElement(this, edge, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new MarkVisibleEdgeEvent(this, edge));
        }
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    public Iterable<Edge> getEdgesFromVertex(final String vertexId, EnumSet<FetchHint> fetchHints, final Long endTime, final Authorizations authorizations) {
        final boolean includeHidden = fetchHints.contains(FetchHint.INCLUDE_HIDDEN);

        return new LookAheadIterable<InMemoryEdge, Edge>() {
            @Override
            protected boolean isIncluded(InMemoryEdge src, Edge edge) {
                String inVertexId = src.getVertexId(Direction.IN);
                checkNotNull(inVertexId, "inVertexId was null");
                String outVertexId = src.getVertexId(Direction.OUT);
                checkNotNull(outVertexId, "outVertexId was null");

                if (!inVertexId.equals(vertexId) && !outVertexId.equals(vertexId)) {
                    return false;
                }

                if (!src.canRead(authorizations)) {
                    return false;
                }

                if (!includeHidden) {
                    if (src.isHidden(authorizations)) {
                        return false;
                    }
                }

                return true;
            }

            @Override
            protected Edge convert(InMemoryEdge edge) {
                return filteredEdge(edge, includeHidden, endTime, authorizations);
            }

            @Override
            protected Iterator<InMemoryEdge> createIterator() {
                return edges.values().iterator();
            }
        };
    }

    private boolean canRead(Visibility visibility, Authorizations authorizations) {
        // this is just a shortcut so that we don't need to construct evaluators and visibility objects to check for an empty string.
        //noinspection SimplifiableIfStatement
        if (visibility.getVisibilityString().length() == 0) {
            return true;
        }

        return authorizations.canRead(visibility);
    }

    public void saveProperties(
            Element element,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            IndexHint indexHint,
            Authorizations authorizations
    ) {
        if (element instanceof Vertex) {
            InMemoryVertex vertex = vertices.get(element.getId());
            vertex.updatePropertiesInternal(properties, propertyDeleteMutations, propertySoftDeleteMutations);
        } else if (element instanceof Edge) {
            InMemoryEdge edge = edges.get(element.getId());
            edge.updatePropertiesInternal(properties, propertyDeleteMutations, propertySoftDeleteMutations);
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + element.getClass().getName());
        }

        if (indexHint != IndexHint.DO_NOT_INDEX) {
            for (PropertyDeleteMutation propertyDeleteMutation : propertyDeleteMutations) {
                getSearchIndex().deleteProperty(
                        this,
                        element,
                        propertyDeleteMutation.getKey(),
                        propertyDeleteMutation.getName(),
                        propertyDeleteMutation.getVisibility(),
                        authorizations
                );
            }
            for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeleteMutations) {
                getSearchIndex().deleteProperty(
                        this,
                        element,
                        propertySoftDeleteMutation.getKey(),
                        propertySoftDeleteMutation.getName(),
                        propertySoftDeleteMutation.getVisibility(),
                        authorizations
                );
            }
            getSearchIndex().addElement(this, element, authorizations);
        }

        if (hasEventListeners()) {
            InMemoryElement inMemoryElement;
            if (element instanceof Vertex) {
                inMemoryElement = vertices.get(element.getId());
            } else {
                inMemoryElement = edges.get(element.getId());
            }
            for (Property property : properties) {
                fireGraphEvent(new AddPropertyEvent(InMemoryGraph.this, inMemoryElement, property));
            }
            for (PropertyDeleteMutation propertyDeleteMutation : propertyDeleteMutations) {
                fireGraphEvent(new DeletePropertyEvent(InMemoryGraph.this, inMemoryElement, propertyDeleteMutation));
            }
            for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeleteMutations) {
                fireGraphEvent(new SoftDeletePropertyEvent(InMemoryGraph.this, inMemoryElement, propertySoftDeleteMutation));
            }
        }
    }

    public void deleteProperty(Element element, Property property, Authorizations authorizations) {
        if (element instanceof Vertex) {
            InMemoryVertex vertex = vertices.get(element.getId());
            vertex.removePropertyInternal(property.getKey(), property.getName());
        } else if (element instanceof Edge) {
            InMemoryEdge edge = edges.get(element.getId());
            edge.removePropertyInternal(property.getKey(), property.getName());
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + element.getClass().getName());
        }
        getSearchIndex().deleteProperty(this, element, property, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new DeletePropertyEvent(this, element, property));
        }
    }

    public void softDeleteProperty(Element element, Property property, Authorizations authorizations) {
        if (element instanceof Vertex) {
            InMemoryVertex vertex = vertices.get(element.getId());
            vertex.softDeletePropertyInternal(property.getKey(), property.getName());
        } else if (element instanceof Edge) {
            InMemoryEdge edge = edges.get(element.getId());
            edge.softDeletePropertyInternal(property.getKey(), property.getName());
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + element.getClass().getName());
        }
        getSearchIndex().deleteProperty(this, element, property, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new SoftDeletePropertyEvent(this, element, property));
        }
    }

    private Edge filteredEdge(InMemoryEdge edge, boolean includeHidden, Long endTime, Authorizations authorizations) {
        String edgeId = edge.getId();
        String outVertexId = edge.getVertexId(Direction.OUT);
        String inVertexId = edge.getVertexId(Direction.IN);
        String label = edge.getLabel();
        Visibility visibility = edge.getVisibility();
        Iterable<Visibility> hiddenVisibilities = edge.getHiddenVisibilities();
        List<Property> properties = filterProperties(edge, edge.getProperties(), includeHidden, endTime, authorizations);
        return new InMemoryEdge(
                this,
                edgeId,
                outVertexId,
                inVertexId,
                label,
                visibility,
                properties,
                edge.getHistoricalPropertyValues(),
                edge.getPropertyDeleteMutations(),
                edge.getPropertySoftDeleteMutations(),
                hiddenVisibilities,
                edge.getStartTime(),
                edge.getTimestamp(),
                authorizations
        );
    }

    private Vertex filteredVertex(InMemoryVertex vertex, boolean includeHidden, Long endTime, Authorizations authorizations) {
        String vertexId = vertex.getId();
        Visibility visibility = vertex.getVisibility();
        Iterable<Visibility> hiddenVisibilities = vertex.getHiddenVisibilities();
        List<Property> properties = filterProperties(vertex, vertex.getProperties(), includeHidden, endTime, authorizations);
        return new InMemoryVertex(
                this,
                vertexId,
                visibility,
                properties,
                vertex.getHistoricalPropertyValues(),
                vertex.getPropertyDeleteMutations(),
                vertex.getPropertySoftDeleteMutations(),
                hiddenVisibilities,
                vertex.getStartTime(),
                vertex.getTimestamp(),
                authorizations
        );
    }

    private List<Property> filterProperties(Element element, Iterable<Property> properties, boolean includeHidden, Long endTime, Authorizations authorizations) {
        List<Property> filteredProperties = new ArrayList<>();
        for (Property p : properties) {
            if (endTime != null) {
                HistoricalPropertyValue historicalPropertyValues = getHistoricalPropertyValue(element, p.getKey(), p.getName(), p.getVisibility(), endTime);
                if (historicalPropertyValues == null) {
                    continue;
                }
                p = new MutablePropertyImpl(
                        p.getKey(),
                        p.getName(),
                        historicalPropertyValues.getValue(),
                        historicalPropertyValues.getMetadata(),
                        historicalPropertyValues.getTimestamp(),
                        historicalPropertyValues.getHiddenVisibilities(),
                        p.getVisibility()
                );
            }

            if (canRead(p.getVisibility(), authorizations) && (includeHidden || !p.isHidden(authorizations))) {
                filteredProperties.add(p);
            }
        }
        return filteredProperties;
    }

    @SuppressWarnings("unused")
    public Map<String, InMemoryVertex> getAllVertices() {
        return this.vertices;
    }

    @SuppressWarnings("unused")
    public Map<String, InMemoryEdge> getAllEdges() {
        return this.edges;
    }

    void alterEdgeVisibility(String edgeId, Visibility newEdgeVisibility) {
        this.edges.get(edgeId).setVisibilityInternal(newEdgeVisibility);
    }

    void alterVertexVisibility(String vertexId, Visibility newVertexVisibility) {
        this.vertices.get(vertexId).setVisibilityInternal(newVertexVisibility);
    }

    void alterEdgePropertyVisibilities(String edgeId, List<AlterPropertyVisibility> alterPropertyVisibilities, Authorizations authorizations) {
        alterElementPropertyVisibilities(this.edges.get(edgeId), alterPropertyVisibilities, authorizations);
    }

    void alterVertexPropertyVisibilities(String vertexId, List<AlterPropertyVisibility> alterPropertyVisibilities, Authorizations authorizations) {
        alterElementPropertyVisibilities(this.vertices.get(vertexId), alterPropertyVisibilities, authorizations);
    }

    void alterElementPropertyVisibilities(InMemoryElement element, List<AlterPropertyVisibility> alterPropertyVisibilities, Authorizations authorizations) {
        for (AlterPropertyVisibility apv : alterPropertyVisibilities) {
            Property property = element.getProperty(apv.getKey(), apv.getName(), apv.getExistingVisibility());
            if (property == null) {
                throw new VertexiumException("Could not find property " + apv.getKey() + ":" + apv.getName());
            }
            Object value = property.getValue();
            Metadata metadata = property.getMetadata();

            element.deleteProperty(apv.getKey(), apv.getName(), authorizations);
            element.addPropertyValue(apv.getKey(), apv.getName(), value, metadata, apv.getVisibility(), authorizations);
        }
    }

    public void alterEdgePropertyMetadata(String edgeId, List<SetPropertyMetadata> setPropertyMetadatas) {
        alterElementPropertyMetadata(this.edges.get(edgeId), setPropertyMetadatas);
    }

    public void alterVertexPropertyMetadata(String vertexId, List<SetPropertyMetadata> setPropertyMetadatas) {
        alterElementPropertyMetadata(this.vertices.get(vertexId), setPropertyMetadatas);
    }

    private void alterElementPropertyMetadata(Element element, List<SetPropertyMetadata> setPropertyMetadatas) {
        for (SetPropertyMetadata apm : setPropertyMetadatas) {
            Property property = element.getProperty(apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility());
            if (property == null) {
                throw new VertexiumException("Could not find property " + apm.getPropertyKey() + ":" + apm.getPropertyName());
            }

            property.getMetadata().add(apm.getMetadataName(), apm.getNewValue(), apm.getMetadataVisibility());
        }
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        return authorizations.canRead(visibility);
    }

    @Override
    public void clearData() {
        this.vertices.clear();
        this.edges.clear();
        getSearchIndex().clearData();
    }

    public void alterEdgeLabel(String edgeId, String newEdgeLabel) {
        InMemoryEdge edge = this.edges.get(edgeId);
        if (edge == null) {
            throw new VertexiumException("Could not find edge " + edgeId);
        }
        edge.setLabel(newEdgeLabel);
    }

    public HistoricalPropertyValue getHistoricalPropertyValue(
            Element element,
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility,
            Long endTime
    ) {
        for (HistoricalPropertyValue hpv : getHistoricalPropertyValues(element, propertyKey, propertyName, propertyVisibility)) {
            if (hpv.getTimestamp() <= endTime) {
                return hpv;
            }
        }
        return null;
    }

    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(
            Element element,
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility
    ) {
        if (element instanceof InMemoryElement) {
            return ((InMemoryElement) element).internalGetHistoricalPropertyValues(propertyKey, propertyName, propertyVisibility);
        } else if (element instanceof Vertex) {
            return this.vertices.get(element.getId()).internalGetHistoricalPropertyValues(propertyKey, propertyName, propertyVisibility);
        } else if (element instanceof Edge) {
            return this.edges.get(element.getId()).internalGetHistoricalPropertyValues(propertyKey, propertyName, propertyVisibility);
        }
        throw new VertexiumException("Unhandled element type: " + element);
    }
}
