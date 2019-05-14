package org.vertexium;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.vertexium.event.GraphEvent;
import org.vertexium.event.GraphEventListener;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.id.IdGenerator;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.query.GraphQuery;
import org.vertexium.query.MultiVertexQuery;
import org.vertexium.query.SimilarToGraphQuery;
import org.vertexium.search.IndexHint;
import org.vertexium.search.SearchIndex;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.Preconditions.checkNotNull;
import static org.vertexium.util.StreamUtils.stream;

@SuppressWarnings("deprecation")
public abstract class GraphBase implements Graph, GraphWithSearchIndex {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(GraphBase.class);
    protected static final VertexiumLogger QUERY_LOGGER = VertexiumLoggerFactory.getQueryLogger(Graph.class);
    public static final String METADATA_DEFINE_PROPERTY_PREFIX = "defineProperty.";
    private final List<GraphEventListener> graphEventListeners = new ArrayList<>();
    private Map<String, PropertyDefinition> propertyDefinitionCache = new ConcurrentHashMap<>();
    private final boolean strictTyping;
    public static final String METADATA_ID_GENERATOR_CLASSNAME = "idGenerator.classname";
    private final GraphConfiguration configuration;
    private final IdGenerator idGenerator;
    private final FetchHints defaultFetchHints;
    private SearchIndex searchIndex;
    private boolean foundIdGeneratorClassnameInMetadata;

    protected GraphBase(GraphConfiguration configuration) {
        this.configuration = configuration;
        this.strictTyping = configuration.isStrictTyping();
        this.searchIndex = configuration.createSearchIndex(this);
        this.idGenerator = configuration.createIdGenerator(this);
        this.defaultFetchHints = FetchHints.ALL;
    }

    protected GraphBase(
        GraphConfiguration configuration,
        IdGenerator idGenerator,
        SearchIndex searchIndex
    ) {
        this.configuration = configuration;
        this.strictTyping = configuration.isStrictTyping();
        this.searchIndex = searchIndex;
        this.idGenerator = idGenerator;
        this.defaultFetchHints = FetchHints.ALL;
    }

    protected void setup() {
        setupGraphMetadata();
    }

    protected void setupGraphMetadata() {
        foundIdGeneratorClassnameInMetadata = false;
        for (GraphMetadataEntry graphMetadataEntry : getMetadata()) {
            setupGraphMetadata(graphMetadataEntry);
        }
        if (!foundIdGeneratorClassnameInMetadata) {
            setMetadata(METADATA_ID_GENERATOR_CLASSNAME, this.idGenerator.getClass().getName());
        }
    }

    protected void setupGraphMetadata(GraphMetadataEntry graphMetadataEntry) {
        if (graphMetadataEntry.getKey().startsWith(METADATA_DEFINE_PROPERTY_PREFIX)) {
            if (graphMetadataEntry.getValue() instanceof PropertyDefinition) {
                addToPropertyDefinitionCache((PropertyDefinition) graphMetadataEntry.getValue());
            } else {
                throw new VertexiumException("Invalid property definition metadata: " + graphMetadataEntry.getKey() + " expected " + PropertyDefinition.class.getName() + " found " + graphMetadataEntry.getValue().getClass().getName());
            }
        } else if (graphMetadataEntry.getKey().equals(METADATA_ID_GENERATOR_CLASSNAME)) {
            if (graphMetadataEntry.getValue() instanceof String) {
                String idGeneratorClassname = (String) graphMetadataEntry.getValue();
                if (idGeneratorClassname.equals(idGenerator.getClass().getName())) {
                    foundIdGeneratorClassnameInMetadata = true;
                }
            } else {
                throw new VertexiumException("Invalid " + METADATA_ID_GENERATOR_CLASSNAME + " expected String found " + graphMetadataEntry.getValue().getClass().getName());
            }
        }
    }

    @Override
    public Stream<RelatedEdge> findRelatedEdgeSummaryForVertices(Iterable<Vertex> verticesIterable, User user) {
        List<RelatedEdge> results = new ArrayList<>();
        List<Vertex> vertices = IterableUtils.toList(verticesIterable);
        for (Vertex outVertex : vertices) {
            outVertex.getEdgeInfos(Direction.OUT, user)
                .forEach(edgeInfo -> {
                    for (Vertex inVertex : vertices) {
                        if (edgeInfo.getVertexId().equals(inVertex.getId())) {
                            results.add(new RelatedEdgeImpl(edgeInfo.getEdgeId(), edgeInfo.getLabel(), outVertex.getId(), inVertex.getId()));
                        }
                    }
                });
        }
        return results.stream();
    }

    @Override
    public Stream<String> findRelatedEdgeIdsForVertices(Iterable<Vertex> verticesIterable, User user) {
        List<String> results = new ArrayList<>();
        List<Vertex> vertices = IterableUtils.toList(verticesIterable);
        for (Vertex outVertex : vertices) {
            if (outVertex == null) {
                throw new VertexiumException("verticesIterable cannot have null values");
            }
            outVertex.getEdgeInfos(Direction.OUT, user)
                .forEach(edgeInfo -> {
                    for (Vertex inVertex : vertices) {
                        if (edgeInfo.getVertexId() == null) { // This check is for legacy data. null EdgeInfo.vertexIds are no longer permitted
                            continue;
                        }
                        if (edgeInfo.getVertexId().equals(inVertex.getId())) {
                            results.add(edgeInfo.getEdgeId());
                        }
                    }
                });
        }
        return results.stream();
    }

    protected abstract GraphMetadataStore getGraphMetadataStore();

    @Override
    public final Iterable<GraphMetadataEntry> getMetadata() {
        return getGraphMetadataStore().getMetadata();
    }

    @Override
    public final void setMetadata(String key, Object value) {
        getGraphMetadataStore().setMetadata(key, value);
    }

    @Override
    public final Object getMetadata(String key) {
        return getGraphMetadataStore().getMetadata(key);
    }

    @Override
    public final Iterable<GraphMetadataEntry> getMetadataWithPrefix(String prefix) {
        return getGraphMetadataStore().getMetadataWithPrefix(prefix);
    }

    @Override
    public void addGraphEventListener(GraphEventListener graphEventListener) {
        this.graphEventListeners.add(graphEventListener);
    }

    protected boolean hasEventListeners() {
        return this.graphEventListeners.size() > 0;
    }

    protected void fireGraphEvent(GraphEvent graphEvent) {
        for (GraphEventListener graphEventListener : this.graphEventListeners) {
            graphEventListener.onGraphEvent(graphEvent);
        }
    }

    @Override
    public Authorizations createAuthorizations(Collection<String> auths) {
        checkNotNull(auths, "auths cannot be null");
        return createAuthorizations(auths.toArray(new String[0]));
    }

    @Override
    public Authorizations createAuthorizations(Authorizations auths, String... additionalAuthorizations) {
        Set<String> newAuths = new HashSet<>();
        Collections.addAll(newAuths, auths.getAuthorizations());
        Collections.addAll(newAuths, additionalAuthorizations);
        return createAuthorizations(newAuths);
    }

    @Override
    public Authorizations createAuthorizations(Authorizations auths, Collection<String> additionalAuthorizations) {
        return createAuthorizations(auths, additionalAuthorizations.toArray(new String[0]));
    }

    @Override
    public long getVertexCount(Authorizations authorizations) {
        return count(getVertices(authorizations));
    }

    @Override
    public long getEdgeCount(Authorizations authorizations) {
        return count(getEdges(authorizations));
    }

    @Override
    public DefinePropertyBuilder defineProperty(String propertyName) {
        return new DefinePropertyBuilder(propertyName) {
            @Override
            public PropertyDefinition define() {
                PropertyDefinition propertyDefinition = super.define();
                savePropertyDefinition(propertyDefinition);
                return propertyDefinition;
            }
        };
    }

    protected void addToPropertyDefinitionCache(PropertyDefinition propertyDefinition) {
        propertyDefinitionCache.put(propertyDefinition.getPropertyName(), propertyDefinition);
    }

    protected void invalidatePropertyDefinition(String propertyName) {
        PropertyDefinition def = (PropertyDefinition) getMetadata(getPropertyDefinitionKey(propertyName));
        if (def == null) {
            propertyDefinitionCache.remove(propertyName);
        } else if (def != null) {
            addToPropertyDefinitionCache(def);
        }
    }

    public void savePropertyDefinition(PropertyDefinition propertyDefinition) {
        addToPropertyDefinitionCache(propertyDefinition);
        setMetadata(getPropertyDefinitionKey(propertyDefinition.getPropertyName()), propertyDefinition);
    }

    private String getPropertyDefinitionKey(String propertyName) {
        return METADATA_DEFINE_PROPERTY_PREFIX + propertyName;
    }

    @Override
    public PropertyDefinition getPropertyDefinition(String propertyName) {
        return propertyDefinitionCache.getOrDefault(propertyName, null);
    }

    @Override
    public Collection<PropertyDefinition> getPropertyDefinitions() {
        return propertyDefinitionCache.values();
    }

    @Override
    public boolean isPropertyDefined(String propertyName) {
        return propertyDefinitionCache.containsKey(propertyName);
    }

    public void ensurePropertyDefined(String name, Object value) {
        PropertyDefinition propertyDefinition = getPropertyDefinition(name);
        if (propertyDefinition != null) {
            return;
        }
        Class<?> valueClass = getValueType(value);
        if (strictTyping) {
            throw new VertexiumTypeException(name, valueClass);
        }
        LOGGER.warn("creating default property definition because a previous definition could not be found for property \"" + name + "\" of type " + valueClass);
        propertyDefinition = new PropertyDefinition(name, valueClass, TextIndexHint.ALL);
        savePropertyDefinition(propertyDefinition);
    }

    protected Class<?> getValueType(Object value) {
        Class<?> valueClass = value.getClass();
        if (value instanceof StreamingPropertyValue) {
            valueClass = ((StreamingPropertyValue) value).getValueType();
        } else if (value instanceof StreamingPropertyValueRef) {
            valueClass = ((StreamingPropertyValueRef) value).getValueType();
        }
        return valueClass;
    }

    @Override
    public List<InputStream> getStreamingPropertyValueInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        return streamingPropertyValues.stream()
            .map(StreamingPropertyValue::getInputStream)
            .collect(Collectors.toList());
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> idsIterable, FetchHints fetchHints, User user) {
        Set<ExtendedDataRowId> ids = Sets.newHashSet(idsIterable);
        return getAllExtendedData(fetchHints, user)
            .filter(row -> ids.contains(row.getId()));
    }

    @Override
    public ExtendedDataRow getExtendedData(ExtendedDataRowId id, User user) {
        List<ExtendedDataRow> rows = getExtendedData(Lists.newArrayList(id), user).collect(Collectors.toList());
        if (rows.size() == 0) {
            return null;
        }
        if (rows.size() == 1) {
            return rows.get(0);
        }
        throw new VertexiumException("Expected 0 or 1 rows found " + rows.size());
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
    public Stream<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, Range elementIdRange, User user) {
        return getAllExtendedData(FetchHints.ALL, user)
            .filter(row -> {
                ExtendedDataRowId rowId = row.getId();
                return elementType.equals(rowId.getElementType())
                    && elementIdRange.isInRange(rowId.getElementId());
            });
    }

    protected Stream<ExtendedDataRow> getAllExtendedData(FetchHints fetchHints, User user) {
        return Stream.concat(getVertices(fetchHints, user), getEdges(fetchHints, user))
            .flatMap(element -> element.getExtendedDataTableNames().stream()
                .flatMap(tableName -> stream(element.getExtendedData(tableName))));
    }

    @SuppressWarnings("deprecation")
    protected void deleteAllExtendedDataForElement(Element element, User user) {
        if (!element.getFetchHints().isIncludeExtendedDataTableNames() || element.getExtendedDataTableNames().size() <= 0) {
            return;
        }

        for (ExtendedDataRow row : getExtendedData(ElementType.getTypeFromElement(element), element.getId(), null, user)) {
            deleteExtendedDataRow(row.getId(), user);
        }
    }

    @Override
    public void visitElements(GraphVisitor graphVisitor, Authorizations authorizations) {
        visitVertices(graphVisitor, authorizations);
        visitEdges(graphVisitor, authorizations);
    }

    @Override
    public void visitVertices(GraphVisitor graphVisitor, Authorizations authorizations) {
        visit(getVertices(authorizations), graphVisitor);
    }

    @Override
    public void visitEdges(GraphVisitor graphVisitor, Authorizations authorizations) {
        visit(getEdges(authorizations), graphVisitor);
    }

    @Override
    public void visit(Iterable<? extends Element> elements, GraphVisitor visitor) {
        int i = 0;
        for (Element element : elements) {
            if (i % 1000000 == 0) {
                LOGGER.debug("checking: %s", element.getId());
            }

            visitor.visitElement(element);
            if (element instanceof Vertex) {
                visitor.visitVertex((Vertex) element);
            } else if (element instanceof Edge) {
                visitor.visitEdge((Edge) element);
            } else {
                throw new VertexiumException("Invalid element type to visit: " + element.getClass().getName());
            }

            for (Property property : element.getProperties()) {
                visitor.visitProperty(element, property);
            }

            for (String tableName : element.getExtendedDataTableNames()) {
                for (ExtendedDataRow extendedDataRow : element.getExtendedData(tableName)) {
                    visitor.visitExtendedDataRow(element, tableName, extendedDataRow);
                    for (Property property : extendedDataRow.getProperties()) {
                        visitor.visitProperty(element, tableName, extendedDataRow, property);
                    }
                }
            }

            i++;
        }
    }

    @Override
    public Stream<HistoricalEvent> getHistoricalEvents(
        Iterable<ElementId> elementIds,
        HistoricalEventId after,
        HistoricalEventsFetchHints fetchHints,
        User user
    ) {
        FetchHints elementFetchHints = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeHidden(true)
            .setIncludeAllEdgeRefs(true)
            .build();
        return fetchHints.applyToResults(stream(elementIds)
            .flatMap(elementId -> {
                Element element = getElement(elementId, elementFetchHints, user);
                if (element == null) {
                    throw new VertexiumException("Could not find: " + elementId);
                }
                return element.getHistoricalEvents(after, fetchHints, user);
            }), after);
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public GraphConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public SearchIndex getSearchIndex() {
        return searchIndex;
    }

    @Override
    public void reindex(Authorizations authorizations) {
        reindexVertices(authorizations);
        reindexEdges(authorizations);
    }

    protected void reindexVertices(Authorizations authorizations) {
        this.searchIndex.addElements(this, getVertices(authorizations), authorizations);
    }

    private void reindexEdges(Authorizations authorizations) {
        this.searchIndex.addElements(this, getEdges(authorizations), authorizations);
    }

    @Override
    public void flush() {
        if (getSearchIndex() != null) {
            this.searchIndex.flush(this);
        }
    }

    @Override
    public void shutdown() {
        flush();
        if (getSearchIndex() != null) {
            this.searchIndex.shutdown();
            this.searchIndex = null;
        }
    }

    @Override
    public GraphQuery query(Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, null, authorizations);
    }

    @Override
    public GraphQuery query(String queryString, Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, queryString, authorizations);
    }

    @Override
    public MultiVertexQuery query(String[] vertexIds, String queryString, Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, vertexIds, queryString, authorizations);
    }

    @Override
    public MultiVertexQuery query(String[] vertexIds, Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, vertexIds, null, authorizations);
    }

    @Override
    public org.vertexium.search.GraphQuery query(String queryString, User user) {
        return getSearchIndex().queryGraph(this, queryString, user);
    }

    @Override
    public org.vertexium.search.MultiVertexQuery query(String[] vertexIds, String queryString, User user) {
        return getSearchIndex().queryGraph(this, vertexIds, queryString, user);
    }

    @Override
    public boolean isQuerySimilarToTextSupported() {
        return getSearchIndex().isQuerySimilarToTextSupported();
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(String[] fields, String text, Authorizations authorizations) {
        return getSearchIndex().querySimilarTo(this, fields, text, authorizations);
    }

    @Override
    public org.vertexium.search.SimilarToGraphQuery querySimilarTo(String[] fields, String text, User user) {
        return getSearchIndex().querySimilarTo(this, fields, text, user);
    }

    @Override
    public boolean isFieldBoostSupported() {
        return getSearchIndex().isFieldBoostSupported();
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return getSearchIndex().getSearchIndexSecurityGranularity();
    }

    @Override
    @Deprecated
    public Map<Object, Long> getVertexPropertyCountByValue(String propertyName, Authorizations authorizations) {
        Map<Object, Long> countsByValue = new HashMap<>();
        for (Vertex v : getVertices(authorizations)) {
            for (Property p : v.getProperties()) {
                if (propertyName.equals(p.getName())) {
                    Object mapKey = p.getValue();
                    if (mapKey instanceof String) {
                        mapKey = ((String) mapKey).toLowerCase();
                    }
                    Long currentValue = countsByValue.get(mapKey);
                    if (currentValue == null) {
                        countsByValue.put(mapKey, 1L);
                    } else {
                        countsByValue.put(mapKey, currentValue + 1);
                    }
                }
            }
        }
        return countsByValue;
    }

    @Override
    public Iterable<Element> saveElementMutations(
        Iterable<ElementMutation<? extends Element>> mutations,
        Authorizations authorizations
    ) {
        List<Element> elements = new ArrayList<>();
        List<Element> elementsToAddToIndex = new ArrayList<>();
        for (ElementMutation<? extends Element> m : mutations) {
            if (m instanceof ExistingElementMutation && !m.hasChanges()) {
                elements.add(((ExistingElementMutation) m).getElement());
                continue;
            }

            IndexHint indexHint = m.getIndexHint();
            m.setIndexHint(IndexHint.DO_NOT_INDEX);
            Element element = m.save(authorizations);
            m.setIndexHint(indexHint);
            elements.add(element);
            if (indexHint == IndexHint.INDEX) {
                elementsToAddToIndex.add(element);
            }
        }
        getSearchIndex().addElements(this, elementsToAddToIndex, authorizations);
        for (ElementMutation<? extends Element> m : mutations) {
            if (m.getIndexHint() == IndexHint.INDEX) {
                getSearchIndex().addElementExtendedData(
                    this,
                    m,
                    m.getExtendedData(),
                    m.getAdditionalExtendedDataVisibilities(),
                    m.getAdditionalExtendedDataVisibilityDeletes(),
                    authorizations
                );
            }
        }
        return elements;
    }

    @Override
    public FetchHints getDefaultFetchHints() {
        return defaultFetchHints;
    }
}
