package org.vertexium;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.vertexium.event.GraphEvent;
import org.vertexium.event.GraphEventListener;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.mutation.ElementMutation;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.query.SimilarToGraphQuery;
import org.vertexium.util.*;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.IterableUtils.count;
import static org.vertexium.util.Preconditions.checkNotNull;
import static org.vertexium.util.StreamUtils.stream;

public abstract class GraphBase implements Graph {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(GraphBase.class);
    protected static final VertexiumLogger QUERY_LOGGER = VertexiumLoggerFactory.getQueryLogger(Graph.class);
    public static final String METADATA_DEFINE_PROPERTY_PREFIX = "defineProperty.";
    private final List<GraphEventListener> graphEventListeners = new ArrayList<>();
    private Map<String, PropertyDefinition> propertyDefinitionCache = new ConcurrentHashMap<>();
    private final boolean strictTyping;

    protected GraphBase(boolean strictTyping) {
        this.strictTyping = strictTyping;
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
    public boolean isQuerySimilarToTextSupported() {
        return false;
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(String[] fields, String text, Authorizations authorizations) {
        throw new VertexiumException("querySimilarTo not supported");
    }

    @Override
    public Authorizations createAuthorizations(Collection<String> auths) {
        checkNotNull(auths, "auths cannot be null");
        return createAuthorizations(auths.toArray(new String[auths.size()]));
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
        return createAuthorizations(auths, additionalAuthorizations.toArray(new String[additionalAuthorizations.size()]));
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
    public Iterable<Element> saveElementMutations(
        Iterable<ElementMutation<? extends Element>> mutations,
        Authorizations authorizations
    ) {
        List<Element> elements = new ArrayList<>();
        for (ElementMutation m : mutations) {
            if (m instanceof ExistingElementMutation && !m.hasChanges()) {
                elements.add(((ExistingElementMutation) m).getElement());
                continue;
            }

            Element element = m.save(authorizations);
            elements.add(element);
        }
        return elements;
    }

    @Override
    public List<InputStream> getStreamingPropertyValueInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        return streamingPropertyValues.stream()
            .map(StreamingPropertyValue::getInputStream)
            .collect(Collectors.toList());
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> idsIterable, FetchHints fetchHints, Authorizations authorizations) {
        Set<ExtendedDataRowId> ids = Sets.newHashSet(idsIterable);
        return new FilterIterable<ExtendedDataRow>(getAllExtendedData(fetchHints, authorizations)) {
            @Override
            protected boolean isIncluded(ExtendedDataRow row) {
                return ids.contains(row.getId());
            }
        };
    }

    @Override
    public ExtendedDataRow getExtendedData(ExtendedDataRowId id, Authorizations authorizations) {
        ArrayList<ExtendedDataRow> rows = Lists.newArrayList(getExtendedData(Lists.newArrayList(id), authorizations));
        if (rows.size() == 0) {
            return null;
        }
        if (rows.size() == 1) {
            return rows.get(0);
        }
        throw new VertexiumException("Expected 0 or 1 rows found " + rows.size());
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedData(
        ElementType elementType,
        String elementId,
        String tableName,
        FetchHints fetchHints,
        Authorizations authorizations
    ) {
        if ((elementType == null && (elementId != null || tableName != null))
            || (elementType != null && elementId == null && tableName != null)) {
            throw new VertexiumException("Cannot create partial key with missing inner value");
        }

        return new FilterIterable<ExtendedDataRow>(getAllExtendedData(fetchHints, authorizations)) {
            @Override
            protected boolean isIncluded(ExtendedDataRow row) {
                ExtendedDataRowId rowId = row.getId();
                return (elementType == null || elementType.equals(rowId.getElementType()))
                    && (elementId == null || elementId.equals(rowId.getElementId()))
                    && (tableName == null || tableName.equals(rowId.getTableName()));
            }
        };
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, Range elementIdRange, Authorizations authorizations) {
        return new FilterIterable<ExtendedDataRow>(getAllExtendedData(FetchHints.ALL, authorizations)) {
            @Override
            protected boolean isIncluded(ExtendedDataRow row) {
                ExtendedDataRowId rowId = row.getId();
                return elementType.equals(rowId.getElementType())
                    && elementIdRange.isInRange(rowId.getElementId());
            }
        };
    }

    protected Iterable<ExtendedDataRow> getAllExtendedData(FetchHints fetchHints, Authorizations authorizations) {
        JoinIterable<Element> allElements = new JoinIterable<>(getVertices(fetchHints, authorizations), getEdges(fetchHints, authorizations));
        return new SelectManyIterable<Element, ExtendedDataRow>(allElements) {
            @Override
            protected Iterable<? extends ExtendedDataRow> getIterable(Element element) {
                return new SelectManyIterable<String, ExtendedDataRow>(element.getExtendedDataTableNames()) {
                    @Override
                    protected Iterable<? extends ExtendedDataRow> getIterable(String tableName) {
                        return element.getExtendedData(tableName);
                    }
                };
            }
        };
    }

    protected void deleteAllExtendedDataForElement(Element element, Authorizations authorizations) {
        if (!element.getFetchHints().isIncludeExtendedDataTableNames() || element.getExtendedDataTableNames().size() <= 0) {
            return;
        }

        for (ExtendedDataRow row : getExtendedData(ElementType.getTypeFromElement(element), element.getId(), null, authorizations)) {
            deleteExtendedDataRow(row.getId(), authorizations);
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
        Authorizations authorizations
    ) {
        FetchHints elementFetchHints = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeHidden(true)
            .setIncludeAllEdgeRefs(true)
            .build();
        return fetchHints.applyToResults(stream(elementIds)
            .flatMap(elementId -> {
                Element element = getElement(elementId, elementFetchHints, authorizations);
                if (element == null) {
                    throw new VertexiumException("Could not find: " + elementId);
                }
                return element.getHistoricalEvents(after, fetchHints, authorizations);
            }), after);
    }
}
