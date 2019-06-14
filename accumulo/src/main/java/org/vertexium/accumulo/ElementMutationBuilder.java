package org.vertexium.accumulo;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.gson.internal.bind.ArrayTypeAdapter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.cache2k.Cache;
import org.cache2k.CacheBuilder;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.keys.KeyHelper;
import org.vertexium.accumulo.util.StreamingPropertyValueStorageStrategy;
import org.vertexium.event.*;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.mutation.*;
import org.vertexium.property.MutableProperty;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.search.IndexHint;
import org.vertexium.util.ArrayUtils;
import org.vertexium.util.ExtendedDataMutationUtils;
import org.vertexium.util.IncreasingTime;
import org.vertexium.util.IterableUtils;

import java.util.*;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.vertexium.mutation.ElementMutationBase.*;
import static org.vertexium.util.IncreasingTime.currentTimeMillis;

public abstract class ElementMutationBuilder {
    public static final Text EMPTY_TEXT = new Text("");
    public static final Value EMPTY_VALUE = new Value("".getBytes());

    private final AccumuloGraph graph;
    private final StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy;
    private final VertexiumSerializer vertexiumSerializer;
    private static final Cache<String, Text> propertyMetadataColumnQualifierTextCache = CacheBuilder
        .newCache(String.class, Text.class)
        .name(ElementMutationBuilder.class, "propertyMetadataColumnQualifierTextCache")
        .maxSize(10000)
        .build();

    protected ElementMutationBuilder(
        AccumuloGraph graph,
        StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy,
        VertexiumSerializer vertexiumSerializer
    ) {
        this.graph = graph;
        this.streamingPropertyValueStorageStrategy = streamingPropertyValueStorageStrategy;
        this.vertexiumSerializer = vertexiumSerializer;
    }

    public void saveVertexMutation(ElementMutation<Vertex> vertexBuilder, long timestamp, User user) {
        String vertexRowKey = vertexBuilder.getId();
        Mutation vertexMutation = new Mutation(vertexRowKey);

        if (!vertexBuilder.isDeleteElement() && !(vertexBuilder instanceof ExistingElementMutation)) {
            Visibility visibility = vertexBuilder.getVisibility();
            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);
            vertexMutation.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, columnVisibility, timestamp, EMPTY_VALUE);
        }
        addElementMutationsToAccumuloMutation(vertexBuilder, vertexRowKey, vertexMutation);

        // We only need the vertex in certain situations, loading it like this lets it be lazy
        Supplier<Vertex> vertex = Suppliers.memoize(() -> getVertexFromMutation(vertexBuilder, user));
        List<Mutation> vertexTableMutations = new ArrayList<>();
        vertexTableMutations.add(vertexMutation);

        if (vertexBuilder instanceof ExistingElementMutation) {
            ExistingElementMutation<Vertex> eem = (ExistingElementMutation<Vertex>) vertexBuilder;
            if (eem.getNewElementVisibility() != null) {
                alterVertexVisibility(
                    vertexMutation,
                    eem.getOldElementVisibility() == null ? vertex.get().getVisibility() : eem.getOldElementVisibility(),
                    eem.getNewElementVisibility(),
                    eem.getNewElementVisibilityData()
                );
            }
        }

        if (vertexBuilder.isDeleteElement()) {
            graph.getSearchIndex().deleteElement(graph, vertex.get(), user);

            FetchHints fetchHints = new FetchHintsBuilder().setIncludeHidden(true).build();
            vertex.get().getEdges(Direction.BOTH, fetchHints, user).forEach(edge -> {
                EdgeMutation edgeMutation = (EdgeMutation) edge.prepareMutation().deleteElement();
                saveEdgeMutation(edgeMutation, timestamp, user);
            });

            vertexMutation.put(AccumuloElement.DELETE_ROW_COLUMN_FAMILY, AccumuloElement.DELETE_ROW_COLUMN_QUALIFIER, RowDeletingIterator.DELETE_ROW_VALUE);
        } else if (vertexBuilder.getSoftDeleteData() != null) {
            graph.getSearchIndex().deleteElement(graph, vertex.get(), user);

            SoftDeleteData data = vertexBuilder.getSoftDeleteData();
            FetchHints fetchHints = new FetchHintsBuilder().setIncludeHidden(true).build();
            vertex.get().getEdges(Direction.BOTH, fetchHints, user).forEach(edge -> {
                EdgeMutation edgeMutation = (EdgeMutation) edge.prepareMutation().softDeleteElement(data.getTimestamp(), data.getEventData());
                saveEdgeMutation(edgeMutation, timestamp, user);
            });

            addSoftDeleteToMutation(vertexMutation, data);
        } else {
            vertexBuilder.getMarkVisibleData().forEach(data -> {
                FetchHints fetchHints = new FetchHintsBuilder().setIncludeHidden(true).build();
                vertex.get().getEdges(Direction.BOTH, fetchHints, user).forEach(edge -> {
                    EdgeMutation edgeMutation = (EdgeMutation) edge.prepareMutation().markElementVisible(data.getVisibility(), data.getEventData()).setIndexHint(vertexBuilder.getIndexHint());
                    saveEdgeMutation(edgeMutation, timestamp, user);
                });
            });
            vertexBuilder.getMarkHiddenData().forEach(data -> {
                vertex.get().getEdges(Direction.BOTH, user).forEach(edge -> {
                    EdgeMutation edgeMutation = (EdgeMutation) edge.prepareMutation().markElementHidden(data.getVisibility(), data.getEventData()).setIndexHint(vertexBuilder.getIndexHint());
                    saveEdgeMutation(edgeMutation, timestamp, user);
                });
            });

            if (vertexBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
                graph.getSearchIndex().addOrUpdateElement(graph, vertexBuilder, user);
            }
        }

        saveVertexMutations(vertexTableMutations.toArray(new Mutation[0]));
        saveExtendedDataMutations(vertexBuilder, user);
        // TODO: In all of the above cases where we delete/modify edges, it would be more efficient to collect all of the vertex mutations and submit them as a batch
        graph.flush();


        queueEvents(vertex, vertexBuilder);
    }

    private Vertex getVertexFromMutation(ElementMutation<Vertex> vertexBuilder, User user) {
        if (vertexBuilder instanceof ExistingElementMutation) {
            // TODO: make sure that we check to see if the fetch hints are what we need if this it delete/softDelete
            return ((ExistingElementMutation<Vertex>) vertexBuilder).getElement();
        }

        FetchHints fetchHints = new FetchHintsBuilder().setIncludeHidden(true).setIncludeAllEdgeRefs(true).build();
        Vertex vertex = graph.getVertex(vertexBuilder.getId(), fetchHints, user);
        if (vertex == null) {
            throw new VertexiumException("Expected to find vertex but was unable to load: " + vertexBuilder.getId());
        }
        return vertex;

    }

    private <T extends Element> void saveExtendedDataMutations(ElementMutation<T> elementBuilder, User user) {
        saveExtendedData(
            elementBuilder,
            elementBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX,
            elementBuilder.getExtendedData(),
            elementBuilder.getExtendedDataDeletes(),
            elementBuilder.getDeleteExtendedDataRowData(),
            elementBuilder.getAdditionalExtendedDataVisibilities(),
            elementBuilder.getAdditionalExtendedDataVisibilityDeletes(),
            user
        );
    }

    private void saveExtendedData(
        ElementLocation elementLocation,
        boolean updateIndex,
        Iterable<ExtendedDataMutation> extendedData,
        Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
        Iterable<ElementMutationBase.DeleteExtendedDataRowData> extendedDataRowDeletes,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        User user
    ) {
        Map<String, Map<String, ExtendedDataMutationUtils.Mutations>> byTableThenRowId = ExtendedDataMutationUtils.getByTableThenRowId(
            extendedData,
            extendedDataDeletes,
            extendedDataRowDeletes,
            additionalExtendedDataVisibilities,
            additionalExtendedDataVisibilityDeletes
        );

        for (Map.Entry<String, Map<String, ExtendedDataMutationUtils.Mutations>> byTableThenRowIdEntry : byTableThenRowId.entrySet()) {
            String tableName = byTableThenRowIdEntry.getKey();
            Map<String, ExtendedDataMutationUtils.Mutations> byRowId = byTableThenRowIdEntry.getValue();
            for (Map.Entry<String, ExtendedDataMutationUtils.Mutations> byRowIdEntry : byRowId.entrySet()) {
                String row = byRowIdEntry.getKey();
                ExtendedDataMutationUtils.Mutations mutations = byRowIdEntry.getValue();

                Mutation m = new Mutation(KeyHelper.createExtendedDataRowKey(elementLocation.getElementType(), elementLocation.getId(), tableName, row));

                if (!mutations.getExtendedDataRowDeletes().isEmpty()) {
                    m.put(AccumuloElement.DELETE_ROW_COLUMN_FAMILY, AccumuloElement.DELETE_ROW_COLUMN_QUALIFIER, RowDeletingIterator.DELETE_ROW_VALUE);
                }

                for (ExtendedDataMutation edm : mutations.getExtendedData()) {
                    Object value = transformValue(edm.getValue(), null, null);

                    // graph can be null if this is running in Map Reduce. We can just assume the property is already defined.
                    if (graph != null) {
                        graph.ensurePropertyDefined(edm.getColumnName(), value);
                    }

                    m.put(
                        AccumuloElement.CF_EXTENDED_DATA,
                        KeyHelper.createExtendedDataColumnQualifier(edm),
                        visibilityToAccumuloVisibility(edm.getVisibility()),
                        new Value(vertexiumSerializer.objectToBytes(value))
                    );
                }

                for (ExtendedDataDeleteMutation eddm : mutations.getExtendedDataDeletes()) {
                    m.putDelete(
                        AccumuloElement.CF_EXTENDED_DATA,
                        KeyHelper.createExtendedDataColumnQualifier(eddm),
                        visibilityToAccumuloVisibility(eddm.getVisibility())
                    );
                }

                for (AdditionalExtendedDataVisibilityAddMutation add : mutations.getAdditionalExtendedDataVisibilities()) {
                    Value value = toAddAdditionalVisibilityValue(add.getEventData());
                    m.put(
                        AccumuloElement.CF_ADDITIONAL_VISIBILITY,
                        new Text(add.getAdditionalVisibility()),
                        new ColumnVisibility(),
                        value
                    );
                }

                for (AdditionalExtendedDataVisibilityDeleteMutation del : mutations.getAdditionalExtendedDataVisibilityDeletes()) {
                    Value value = toDeleteAdditionalVisibilityValue(del.getEventData());
                    m.put(
                        AccumuloElement.CF_ADDITIONAL_VISIBILITY,
                        new Text(del.getAdditionalVisibility()),
                        new ColumnVisibility(),
                        value
                    );
                }

                saveExtendedDataMutation(elementLocation.getElementType(), m);
            }
        }

        if (updateIndex) {
            graph.getSearchIndex().addElementExtendedData(
                graph,
                elementLocation,
                extendedData,
                additionalExtendedDataVisibilities,
                additionalExtendedDataVisibilityDeletes,
                user
            );
            for (ExtendedDataDeleteMutation m : extendedDataDeletes) {
                graph.getSearchIndex().deleteExtendedData(
                    graph,
                    elementLocation,
                    m.getTableName(),
                    m.getRow(),
                    m.getColumnName(),
                    m.getKey(),
                    m.getVisibility(),
                    user
                );
            }
        }
    }

    protected abstract void saveExtendedDataMutation(ElementType elementType, Mutation m);

    protected abstract void saveVertexMutations(Mutation... m);

    <T extends Element> void addElementMutationsToAccumuloMutation(ElementMutation<T> elementMutation, String rowKey, Mutation m) {
        if (elementMutation.isDeleteElement()) {
            return;
        }

        for (PropertyDeleteMutation propertyDeleteMutation : elementMutation.getPropertyDeletes()) {
            addPropertyDeleteToMutation(m, propertyDeleteMutation);
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : elementMutation.getPropertySoftDeletes()) {
            addPropertySoftDeleteToMutation(m, propertySoftDeleteMutation);
        }
        for (Property property : elementMutation.getProperties()) {
            addPropertyToMutation(m, rowKey, property);
        }

        Supplier<Element> element = Suppliers.memoize(() -> getElementFromMutation(elementMutation));
        for (SetPropertyMetadata propertyMetadata : elementMutation.getSetPropertyMetadata()) {
            Property property = element.get().getProperty(propertyMetadata.getPropertyKey(), propertyMetadata.getPropertyName(), propertyMetadata.getPropertyVisibility());
            if (property == null) {
                throw new VertexiumException("Unable to load existing property for AlterPropertyVisibility:" + propertyMetadata);
            }
            Visibility propertyVisibility = propertyMetadata.getPropertyVisibility();
            if (propertyVisibility == null) {
                propertyVisibility = property.getVisibility();
            }
            addPropertyMetadataItemToMutation(
                m,
                propertyMetadata.getPropertyName(),
                propertyMetadata.getPropertyKey(),
                propertyVisibility,
                propertyMetadata.getMetadataName(),
                propertyMetadata.getNewValue(),
                property.getTimestamp(),
                propertyMetadata.getMetadataVisibility()
            );
        }
        for (AdditionalVisibilityAddMutation additionalVisibility : elementMutation.getAdditionalVisibilities()) {
            addAdditionalVisibilityToMutation(m, additionalVisibility);
        }
        for (AdditionalVisibilityDeleteMutation additionalVisibilityDelete : elementMutation.getAdditionalVisibilityDeletes()) {
            addAdditionalVisibilityDeleteToMutation(m, additionalVisibilityDelete);
        }

        for (AlterPropertyVisibility alterPropertyVisibility : elementMutation.getAlterPropertyVisibilities()) {
            Property property = element.get().getProperty(alterPropertyVisibility.getKey(), alterPropertyVisibility.getName(), alterPropertyVisibility.getExistingVisibility());
            if (property == null) {
                throw new VertexiumException("Unable to load existing property for AlterPropertyVisibility:" + alterPropertyVisibility);
            }
            addPropertySoftDeleteToMutation(
                m,
                alterPropertyVisibility.getKey(),
                alterPropertyVisibility.getName(),
                property.getVisibility(),
                alterPropertyVisibility.getTimestamp() - 1,
                alterPropertyVisibility.getData()
            );

            MutablePropertyImpl newProperty = new MutablePropertyImpl(
                property.getKey(),
                property.getName(),
                property.getValue(),
                property.getMetadata(),
                alterPropertyVisibility.getTimestamp(),
                IterableUtils.toSet(property.getHiddenVisibilities()),
                alterPropertyVisibility.getVisibility(),
                property.getFetchHints()
            );
            addPropertyToMutation(m, rowKey, newProperty);
        }

        // TODO: handle cleaning up the extended data markers for extended data deletions. need to elevate to do this properly

        Iterable<ExtendedDataMutation> extendedData = elementMutation.getExtendedData();
        saveExtendedDataMarkers(m, extendedData);

        for (MarkPropertyVisibleData markPropertyVisibleData : elementMutation.getMarkPropertyVisibleData()) {
            addMarkPropertyVisibleToMutation(m, markPropertyVisibleData);
        }
        for (MarkPropertyHiddenData markPropertyHiddenData : elementMutation.getMarkPropertyHiddenData()) {
            addMarkPropertyHiddenToMutation(m, markPropertyHiddenData);
        }
        for (MarkVisibleData markVisibleData : elementMutation.getMarkVisibleData()) {
            addMarkVisibleToMutation(m, markVisibleData);
        }
        for (MarkHiddenData markHiddenData : elementMutation.getMarkHiddenData()) {
            addMarkHiddenToMutation(m, markHiddenData);
        }
    }

    private <T extends Element>  Element getElementFromMutation(ElementMutation<T> elementMutation) {
        // TODO: if it's not an ExistingElementMutation, should we go get the element?
        if (!(elementMutation instanceof ExistingElementMutation)) {
            throw new VertexiumException("Altering property visibility requires using an ExistingElementMutation");
        }
        return ((ExistingElementMutation)elementMutation).getElement();
    }

    private void saveExtendedDataMarkers(Mutation m, Iterable<ExtendedDataMutation> extendedData) {
        for (TableNameVisibilityPair pair : TableNameVisibilityPair.getUniquePairs(extendedData)) {
            addExtendedDataMarkerToElementMutation(m, pair);
        }
    }

    private void addExtendedDataMarkerToElementMutation(Mutation m, TableNameVisibilityPair pair) {
        m.put(
            AccumuloElement.CF_EXTENDED_DATA,
            new Text(pair.getTableName()),
            visibilityToAccumuloVisibility(pair.getVisibility()),
            new Value(pair.getTableName().getBytes())
        );
    }

    private Object transformValue(Object propertyValue, String rowKey, Property property) {
        if (propertyValue instanceof StreamingPropertyValue) {
            if (rowKey != null && property != null) {
                propertyValue = saveStreamingPropertyValue(rowKey, property, (StreamingPropertyValue) propertyValue);
            } else {
                throw new VertexiumException(StreamingPropertyValue.class.getSimpleName() + " are not supported");
            }
        }
        if (propertyValue instanceof DateOnly) {
            propertyValue = ((DateOnly) propertyValue).getDate();
        }
        return propertyValue;
    }

    @SuppressWarnings("unchecked")
    public void saveEdgeMutation(EdgeMutation edgeBuilder, long timestamp, User user) {
        String edgeRowKey = edgeBuilder.getId();
        Text edgeIdText = new Text(edgeBuilder.getId());
        Mutation edgeMutation = new Mutation(edgeRowKey);
        Mutation outMutation = new Mutation(edgeBuilder.getVertexId(Direction.OUT));
        Mutation inMutation = new Mutation(edgeBuilder.getVertexId(Direction.IN));
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(edgeBuilder.getVisibility());

        String edgeLabel = edgeBuilder.getNewEdgeLabel() != null ? edgeBuilder.getNewEdgeLabel() : edgeBuilder.getEdgeLabel();
        if (!edgeBuilder.isDeleteElement()) {
            if (edgeBuilder instanceof ExistingElementMutation) {
                ExistingElementMutation<Edge> eem = (ExistingElementMutation<Edge>) edgeBuilder;
                Visibility oldVisibility = eem.getOldElementVisibility() == null ? eem.getVisibility() : eem.getOldElementVisibility();
                if (eem.getNewElementVisibility() != null && !oldVisibility.equals(eem.getNewElementVisibility())) {
                    alterEdgeVisibility(
                        edgeMutation,
                        eem.getElement(),
                        oldVisibility,
                        eem.getNewElementVisibility(),
                        eem.getNewElementVisibilityData()
                    );

                    outMutation.putDelete(AccumuloVertex.CF_OUT_EDGE, edgeIdText, edgeColumnVisibility);
                    inMutation.putDelete(AccumuloVertex.CF_IN_EDGE, edgeIdText, edgeColumnVisibility);

                    edgeColumnVisibility = visibilityToAccumuloVisibility(eem.getNewElementVisibility());

                    Text edgeInfoVisibility = new Text(edgeColumnVisibility.getExpression());
                    EdgeInfo edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edgeBuilder.getEdgeLabel()), edgeBuilder.getVertexId(Direction.IN), edgeInfoVisibility);
                    outMutation.put(AccumuloVertex.CF_OUT_EDGE, edgeIdText, edgeColumnVisibility, timestamp, edgeInfo.toValue());

                    edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edgeBuilder.getEdgeLabel()), edgeBuilder.getVertexId(Direction.OUT), edgeInfoVisibility);
                    inMutation.put(AccumuloVertex.CF_IN_EDGE, edgeIdText, edgeColumnVisibility, timestamp, edgeInfo.toValue());
                }
            }

            if (!(edgeBuilder instanceof ExistingElementMutation) || edgeBuilder.getNewEdgeLabel() != null) {
                edgeMutation.put(AccumuloEdge.CF_SIGNAL, new Text(edgeLabel), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
                edgeMutation.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edgeBuilder.getVertexId(Direction.OUT)), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
                edgeMutation.put(AccumuloEdge.CF_IN_VERTEX, new Text(edgeBuilder.getVertexId(Direction.IN)), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);

                Text edgeInfoVisibility = new Text(edgeColumnVisibility.getExpression());
                EdgeInfo edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), edgeBuilder.getVertexId(Direction.IN), edgeInfoVisibility);
                outMutation.put(AccumuloVertex.CF_OUT_EDGE, edgeIdText, edgeColumnVisibility, timestamp, edgeInfo.toValue());

                edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), edgeBuilder.getVertexId(Direction.OUT), edgeInfoVisibility);
                inMutation.put(AccumuloVertex.CF_IN_EDGE, edgeIdText, edgeColumnVisibility, timestamp, edgeInfo.toValue());
            }
        }
        addElementMutationsToAccumuloMutation(edgeBuilder, edgeRowKey, edgeMutation);

        // We only need the edge in certain situations, loading it like this lets it be lazy
        Supplier<Edge> edge = Suppliers.memoize(() -> getEdgeFromMutation(edgeBuilder, user));
        if (edgeBuilder.isDeleteElement()) {
            graph.getSearchIndex().deleteElement(graph, edge.get(), user);

            outMutation.putDelete(AccumuloVertex.CF_OUT_EDGE, edgeIdText, edgeColumnVisibility);
            inMutation.putDelete(AccumuloVertex.CF_IN_EDGE, edgeIdText, edgeColumnVisibility);

            Mutation deleteMutation = new Mutation(edgeRowKey);
            deleteMutation.put(AccumuloElement.DELETE_ROW_COLUMN_FAMILY, AccumuloElement.DELETE_ROW_COLUMN_QUALIFIER, RowDeletingIterator.DELETE_ROW_VALUE);
            saveEdgeMutations(deleteMutation);
        } else if (edgeBuilder.getSoftDeleteData() != null) {
            graph.getSearchIndex().deleteElement(graph, edge.get(), user);

            SoftDeleteData softDeleteData = edgeBuilder.getSoftDeleteData();

            Long softDeleteTimestamp = softDeleteData.getTimestamp();
            if (softDeleteTimestamp == null) {
                softDeleteTimestamp = IncreasingTime.currentTimeMillis();
            }
            Value value = toSoftDeleteDataToValue(softDeleteData.getEventData());
            outMutation.put(AccumuloVertex.CF_OUT_EDGE_SOFT_DELETE, edgeIdText, edgeColumnVisibility, softDeleteTimestamp, value);
            inMutation.put(AccumuloVertex.CF_IN_EDGE_SOFT_DELETE, edgeIdText, edgeColumnVisibility, softDeleteTimestamp, value);

            Mutation softDeleteMutation = new Mutation(edgeRowKey);
            addSoftDeleteToMutation(softDeleteMutation, softDeleteData);
            saveEdgeMutations(softDeleteMutation);
        } else {
            edgeBuilder.getMarkVisibleData().forEach(data -> {
                Value value = toHiddenDeletedValue(data.getEventData());
                ColumnVisibility hiddenVisibility = visibilityToAccumuloVisibility(data.getVisibility());
                outMutation.put(AccumuloVertex.CF_OUT_EDGE_HIDDEN, edgeIdText, hiddenVisibility, timestamp, value);
                inMutation.put(AccumuloVertex.CF_IN_EDGE_HIDDEN, edgeIdText, hiddenVisibility, timestamp, value);
            });
            edgeBuilder.getMarkHiddenData().forEach(data -> {
                Value value = toHiddenValue(data.getEventData());
                ColumnVisibility hiddenVisibility = visibilityToAccumuloVisibility(data.getVisibility());
                outMutation.put(AccumuloVertex.CF_OUT_EDGE_HIDDEN, edgeIdText, hiddenVisibility, timestamp, value);
                inMutation.put(AccumuloVertex.CF_IN_EDGE_HIDDEN, edgeIdText, hiddenVisibility, timestamp, value);
            });

            if (edgeBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
                graph.getSearchIndex().addOrUpdateElement(graph, edgeBuilder, user);
            }
        }
        saveEdgeMutations(edgeMutation);
        saveVertexMutations(outMutation, inMutation);
        saveExtendedDataMutations(edgeBuilder, user);

        graph.flush();
        queueEvents(edge, edgeBuilder);
    }

    @SuppressWarnings("unchecked")
    private Edge getEdgeFromMutation(EdgeMutation edgeBuilder, User user) {
        if (edgeBuilder instanceof ExistingElementMutation) {
            return ((ExistingElementMutation<Edge>) edgeBuilder).getElement();
        }

        Edge edge = graph.getEdge(edgeBuilder.getId(), FetchHints.NONE, user);
        if (edge == null) {
            throw new VertexiumException("Expected to find edge but was unable to load: " + edgeBuilder.getId());
        }
        return edge;
    }

    private ColumnVisibility visibilityToAccumuloVisibility(Visibility visibility) {
        return new ColumnVisibility(visibility.getVisibilityString());
    }

    protected abstract void saveEdgeMutations(Mutation... m);

    public void alterVertexVisibility(Mutation m, Visibility oldVisibility, Visibility newVisibility, Object data) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(oldVisibility);
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (!currentColumnVisibility.equals(newColumnVisibility)) {
            m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, currentColumnVisibility, currentTimeMillis(), toSignalDeletedValue(data));
            m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, newColumnVisibility, currentTimeMillis(), toSignalValue(data));
        }
    }

    public void alterEdgeVisibility(Mutation m, Edge edge, Visibility oldVisibility, Visibility newVisibility, Object data) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(oldVisibility);
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (!currentColumnVisibility.equals(newColumnVisibility)) {
            m.put(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), currentColumnVisibility, currentTimeMillis(), toSignalDeletedValue(data));
            m.put(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), newColumnVisibility, currentTimeMillis(), toSignalValue(data));

            m.putDelete(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), currentColumnVisibility, currentTimeMillis());
            m.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), newColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);

            m.putDelete(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), currentColumnVisibility, currentTimeMillis());
            m.put(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), newColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);
        }
    }

    public boolean alterEdgeVertexOutVertex(Mutation vertexOutMutation, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        Text newColumnVisibilityAsText = new Text(newColumnVisibility.getExpression());
        EdgeInfo edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edge.getLabel()), edge.getVertexId(Direction.IN), newColumnVisibilityAsText);
        vertexOutMutation.putDelete(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), currentColumnVisibility);
        vertexOutMutation.put(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), newColumnVisibility, edgeInfo.toValue());
        return true;
    }

    public boolean alterEdgeVertexInVertex(Mutation vertexInMutation, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        Text newColumnVisibilityAsText = new Text(newColumnVisibility.getExpression());
        EdgeInfo edgeInfo = new EdgeInfo(getNameSubstitutionStrategy().deflate(edge.getLabel()), edge.getVertexId(Direction.OUT), newColumnVisibilityAsText);
        vertexInMutation.putDelete(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), currentColumnVisibility);
        vertexInMutation.put(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), newColumnVisibility, edgeInfo.toValue());
        return true;
    }

    public void addAdditionalVisibilityToMutation(
        Mutation m,
        AdditionalVisibilityAddMutation additionalVisibility
    ) {
        Value value = toAddAdditionalVisibilityValue(additionalVisibility.getEventData());
        m.put(AccumuloElement.CF_ADDITIONAL_VISIBILITY, new Text(additionalVisibility.getAdditionalVisibility()), new ColumnVisibility(), value);
    }

    public void addAdditionalVisibilityDeleteToMutation(
        Mutation m,
        AdditionalVisibilityDeleteMutation additionalVisibilityDelete
    ) {
        Value value = toDeleteAdditionalVisibilityValue(additionalVisibilityDelete.getEventData());
        m.put(AccumuloElement.CF_ADDITIONAL_VISIBILITY, new Text(additionalVisibilityDelete.getAdditionalVisibility()), new ColumnVisibility(), value);
    }

    public void addMarkPropertyHiddenToMutation(Mutation m, MarkPropertyHiddenData markPropertyHiddenData) {
        Visibility visibility = markPropertyHiddenData.getVisibility();
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(
            markPropertyHiddenData.getKey(),
            markPropertyHiddenData.getName(),
            markPropertyHiddenData.getPropertyVisibility().getVisibilityString(),
            getNameSubstitutionStrategy()
        );
        Long timestamp = markPropertyHiddenData.getTimestamp();
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        Object data = markPropertyHiddenData.getEventData();
        m.put(AccumuloElement.CF_PROPERTY_HIDDEN, columnQualifier, columnVisibility, timestamp, toHiddenValue(data));
    }

    public void addMarkPropertyVisibleToMutation(Mutation m, MarkPropertyVisibleData markPropertyVisibleData) {
        Visibility visibility = markPropertyVisibleData.getVisibility();
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(
            markPropertyVisibleData.getKey(),
            markPropertyVisibleData.getName(),
            markPropertyVisibleData.getPropertyVisibility().getVisibilityString(),
            getNameSubstitutionStrategy());
        Long timestamp = markPropertyVisibleData.getTimestamp();
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        Object data = markPropertyVisibleData.getEventData();
        m.put(AccumuloElement.CF_PROPERTY_HIDDEN, columnQualifier, columnVisibility, timestamp, toHiddenDeletedValue(data));
    }

    public void addMarkVisibleToMutation(Mutation m, MarkVisibleData markVisibleData) {
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(markVisibleData.getVisibility());
        Object data = markVisibleData.getEventData();
        Long timestamp = markVisibleData.getTimestamp();
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        m.put(AccumuloElement.CF_HIDDEN, AccumuloElement.CQ_HIDDEN, columnVisibility, timestamp, toHiddenDeletedValue(data));
    }

    public void addMarkHiddenToMutation(Mutation m, MarkHiddenData markHiddenData) {
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(markHiddenData.getVisibility());
        Object data = markHiddenData.getEventData();
        Long timestamp = markHiddenData.getTimestamp();
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        m.put(AccumuloElement.CF_HIDDEN, AccumuloElement.CQ_HIDDEN, columnVisibility, timestamp, toHiddenValue(data));
    }

    public void addSoftDeleteToMutation(Mutation m, SoftDeleteData softDeleteData) {
        Object data = softDeleteData.getEventData();
        Long timestamp = softDeleteData.getTimestamp();
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        m.put(AccumuloElement.CF_SOFT_DELETE, AccumuloElement.CQ_SOFT_DELETE, timestamp, toSoftDeleteDataToValue(data));
    }

    public void addPropertyToMutation(Mutation m, String rowKey, Property property) {
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(property.getKey(), property.getName(), getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        Object propertyValue = transformValue(property.getValue(), rowKey, property);

        // graph can be null if this is running in Map Reduce. We can just assume the property is already defined.
        if (graph != null) {
            graph.ensurePropertyDefined(property.getName(), propertyValue);
        }

        Value value = new Value(vertexiumSerializer.objectToBytes(propertyValue));
        m.put(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, property.getTimestamp(), value);
        addPropertyMetadataToMutation(m, property);
    }

    protected abstract NameSubstitutionStrategy getNameSubstitutionStrategy();

    public void addPropertyDeleteToMutation(Mutation m, PropertyDeleteMutation propertyDelete) {
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(propertyDelete.getKey(), propertyDelete.getName(), getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(propertyDelete.getVisibility());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, currentTimeMillis());
        addPropertyDeleteMetadataToMutation(m, propertyDelete);
    }

    public void addPropertyMetadataToMutation(Mutation m, Property property) {
        Metadata metadata = property.getMetadata();
        for (Metadata.Entry metadataItem : metadata.entrySet()) {
            addPropertyMetadataItemToMutation(
                m,
                property,
                metadataItem.getKey(),
                metadataItem.getValue(),
                metadataItem.getVisibility()
            );
        }
    }

    public void addPropertyMetadataItemToMutation(
        Mutation m,
        Property property,
        String metadataKey,
        Object metadataValue,
        Visibility visibility
    ) {
        Text columnQualifier = getPropertyMetadataColumnQualifierText(property, metadataKey);
        addPropertyMetadataItemToMutation(m, columnQualifier, metadataValue, property.getTimestamp(), visibility);
    }

    public void addPropertyMetadataItemToMutation(
        Mutation m,
        String propertyName,
        String propertyKey,
        Visibility propertyVisibility,
        String metadataKey,
        Object metadataValue,
        Long timestamp,
        Visibility visibility
    ) {
        Text columnQualifier = getPropertyMetadataColumnQualifierText(propertyName, propertyKey, propertyVisibility, metadataKey);
        addPropertyMetadataItemToMutation(m, columnQualifier, metadataValue, timestamp, visibility);
    }

    private void addPropertyMetadataItemToMutation(
        Mutation m,
        Text columnQualifier,
        Object metadataValue,
        Long timestamp,
        Visibility visibility
    ) {

        ColumnVisibility metadataVisibility = visibilityToAccumuloVisibility(visibility);
        if (metadataValue == null) {
            addPropertyMetadataItemDeleteToMutation(m, columnQualifier, metadataVisibility);
        } else {
            addPropertyMetadataItemAddToMutation(m, columnQualifier, metadataVisibility, timestamp, metadataValue);
        }
    }

    private void addPropertyMetadataItemAddToMutation(Mutation m, Text columnQualifier, ColumnVisibility metadataVisibility, long propertyTimestamp, Object value) {
        Value metadataValue = new Value(vertexiumSerializer.objectToBytes(value));
        m.put(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, propertyTimestamp, metadataValue);
    }

    private void addPropertyMetadataItemDeleteToMutation(Mutation m, Text columnQualifier, ColumnVisibility metadataVisibility) {
        m.putDelete(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, currentTimeMillis());
    }

    private Text getPropertyMetadataColumnQualifierText(Property property, String metadataKey) {
        return getPropertyMetadataColumnQualifierText(property.getName(), property.getKey(), property.getVisibility(), metadataKey);
    }

    private Text getPropertyMetadataColumnQualifierText(String propertyName, String propertyKey, Visibility propertyVisibility, String metadataKey) {
        String visibilityString = propertyVisibility.getVisibilityString();
        //noinspection StringBufferReplaceableByString - for speed we use StringBuilder
        StringBuilder keyBuilder = new StringBuilder(propertyName.length() + propertyKey.length() + visibilityString.length() + metadataKey.length());
        keyBuilder.append(getNameSubstitutionStrategy().deflate(propertyName));
        keyBuilder.append(getNameSubstitutionStrategy().deflate(propertyKey));
        keyBuilder.append(visibilityString);
        keyBuilder.append(getNameSubstitutionStrategy().deflate(metadataKey));
        String key = keyBuilder.toString();
        Text r = propertyMetadataColumnQualifierTextCache.peek(key);
        if (r == null) {
            r = KeyHelper.getColumnQualifierFromPropertyMetadataColumnQualifier(propertyName, propertyKey, visibilityString, metadataKey, getNameSubstitutionStrategy());
            propertyMetadataColumnQualifierTextCache.put(key, r);
        }
        return r;
    }

    public void addPropertyDeleteMetadataToMutation(Mutation m, PropertyDeleteMutation propertyDeleteMutation) {
        if (propertyDeleteMutation instanceof PropertyPropertyDeleteMutation) {
            Property property = ((PropertyPropertyDeleteMutation) propertyDeleteMutation).getProperty();
            Metadata metadata = property.getMetadata();
            for (Metadata.Entry metadataItem : metadata.entrySet()) {
                Text columnQualifier = getPropertyMetadataColumnQualifierText(property, metadataItem.getKey());
                ColumnVisibility metadataVisibility = visibilityToAccumuloVisibility(metadataItem.getVisibility());
                addPropertyMetadataItemDeleteToMutation(m, columnQualifier, metadataVisibility);
            }
        }
    }

    protected StreamingPropertyValueRef saveStreamingPropertyValue(final String rowKey, final Property property, StreamingPropertyValue propertyValue) {
        return streamingPropertyValueStorageStrategy.saveStreamingPropertyValue(this, rowKey, property, propertyValue);
    }

    public void addPropertySoftDeleteToMutation(Mutation m, Property property, long timestamp, Object data) {
        addPropertySoftDeleteToMutation(
            m,
            property.getKey(),
            property.getName(),
            property.getVisibility(),
            timestamp,
            data
        );
    }

    private void addPropertySoftDeleteToMutation(Mutation m, PropertySoftDeleteMutation propertySoftDelete) {
        addPropertySoftDeleteToMutation(
            m,
            propertySoftDelete.getKey(),
            propertySoftDelete.getName(),
            propertySoftDelete.getVisibility(),
            propertySoftDelete.getTimestamp(),
            propertySoftDelete.getData()
        );
    }

    private void addPropertySoftDeleteToMutation(Mutation m, String key, String name, Visibility visibility, Long timestamp, Object data) {
        Text columnQualifier = KeyHelper.getColumnQualifierFromPropertyColumnQualifier(key, name, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);
        m.put(AccumuloElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, timestamp, toSoftDeleteDataToValue(data));
    }

    public abstract void saveDataMutation(Mutation dataMutation);

    public Value toSoftDeleteDataToValue(Object data) {
        if (data == null) {
            return AccumuloElement.SOFT_DELETE_VALUE;
        }
        // to support un-soft delete in the future prevent the use of values starting with
        byte[] valueDeletedArray = AccumuloElement.SOFT_DELETE_VALUE_DELETED.get();
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        if (ArrayUtils.startsWith(dataArray, valueDeletedArray)) {
            throw new VertexiumException("Soft delete value data cannot start with soft delete value deleted marker: " + AccumuloElement.SOFT_DELETE_VALUE_DELETED.toString());
        }
        return new Value(dataArray);
    }

    private Value toHiddenDeletedValue(Object data) {
        if (data == null) {
            return AccumuloElement.HIDDEN_VALUE_DELETED;
        }
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        byte[] valueDeletedArray = AccumuloElement.HIDDEN_VALUE_DELETED.get();
        byte[] value = new byte[valueDeletedArray.length + dataArray.length];
        System.arraycopy(valueDeletedArray, 0, value, 0, valueDeletedArray.length);
        System.arraycopy(dataArray, 0, value, valueDeletedArray.length, dataArray.length);
        return new Value(value);
    }

    private Value toHiddenValue(Object data) {
        if (data == null) {
            return AccumuloElement.HIDDEN_VALUE;
        }
        byte[] valueDeletedArray = AccumuloElement.HIDDEN_VALUE_DELETED.get();
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        if (ArrayUtils.startsWith(dataArray, valueDeletedArray)) {
            throw new VertexiumException("Hidden value data cannot start with hidden value deleted marker: " + AccumuloElement.HIDDEN_VALUE_DELETED.toString());
        }
        return new Value(dataArray);
    }

    private Value toAddAdditionalVisibilityValue(Object data) {
        if (data == null) {
            return AccumuloElement.ADD_ADDITIONAL_VISIBILITY_VALUE;
        }
        byte[] valueDeletedArray = AccumuloElement.ADD_ADDITIONAL_VISIBILITY_VALUE_DELETED.get();
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        if (ArrayUtils.startsWith(dataArray, valueDeletedArray)) {
            throw new VertexiumException("Add additional visibility value data cannot start with hidden value deleted marker: " + AccumuloElement.ADD_ADDITIONAL_VISIBILITY_VALUE_DELETED.toString());
        }
        return new Value(dataArray);
    }

    private Value toDeleteAdditionalVisibilityValue(Object data) {
        if (data == null) {
            return AccumuloElement.ADD_ADDITIONAL_VISIBILITY_VALUE_DELETED;
        }
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        byte[] valueDeletedArray = AccumuloElement.ADD_ADDITIONAL_VISIBILITY_VALUE_DELETED.get();
        byte[] value = new byte[valueDeletedArray.length + dataArray.length];
        System.arraycopy(valueDeletedArray, 0, value, 0, valueDeletedArray.length);
        System.arraycopy(dataArray, 0, value, valueDeletedArray.length, dataArray.length);
        return new Value(value);
    }

    private Value toSignalDeletedValue(Object data) {
        if (data == null) {
            return AccumuloElement.SIGNAL_VALUE_DELETED;
        }
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        byte[] valueDeletedArray = AccumuloElement.SIGNAL_VALUE_DELETED.get();
        byte[] value = new byte[valueDeletedArray.length + dataArray.length];
        System.arraycopy(valueDeletedArray, 0, value, 0, valueDeletedArray.length);
        System.arraycopy(dataArray, 0, value, valueDeletedArray.length, dataArray.length);
        return new Value(value);
    }

    private Value toSignalValue(Object data) {
        if (data == null) {
            return ElementMutationBuilder.EMPTY_VALUE;
        }
        byte[] valueDeletedArray = AccumuloElement.SIGNAL_VALUE_DELETED.get();
        byte[] dataArray = vertexiumSerializer.objectToBytes(data);
        if (ArrayUtils.startsWith(dataArray, valueDeletedArray)) {
            throw new VertexiumException("Signal value data cannot start with delete value marker: " + AccumuloElement.SIGNAL_VALUE_DELETED.toString());
        }
        return new Value(dataArray);
    }

    private void queueEvents(
        Supplier<? extends Element> element,
        ElementMutation<? extends Element> mutation
    ) {
        if (!(mutation instanceof ExistingElementMutation)) {
            if (element.get() instanceof Edge) {
                graph.queueEvent(new AddEdgeEvent(graph, (Edge) element.get()));
            } else if (element.get() instanceof Vertex) {
                graph.queueEvent(new AddVertexEvent(graph, (Vertex) element.get()));
            } else {
                throw new VertexiumException("Unexpected element type: " + element.get());
            }
        }

        mutation.getProperties().forEach(property ->
            graph.queueEvent(new AddPropertyEvent(graph, element.get(), property))
        );
        mutation.getPropertyDeletes().forEach(propertyDeleteMutation ->
            graph.queueEvent(new DeletePropertyEvent(graph, element.get(), propertyDeleteMutation))
        );
        mutation.getAlterPropertyVisibilities().forEach(alterPropertyVisibility -> {
            Property property = element.get().getProperty(alterPropertyVisibility.getKey(), alterPropertyVisibility.getName(), alterPropertyVisibility.getExistingVisibility());
            if (property == null) {
                throw new VertexiumException("Unable to load existing property for AlterPropertyVisibility:" + alterPropertyVisibility);
            }
            graph.queueEvent(new SoftDeletePropertyEvent(graph, element.get(), property.getKey(), property.getName(), property.getVisibility(), alterPropertyVisibility.getData()));

            MutablePropertyImpl newProperty = new MutablePropertyImpl(
                property.getKey(),
                property.getName(),
                property.getValue(),
                property.getMetadata(),
                alterPropertyVisibility.getTimestamp(),
                IterableUtils.toSet(property.getHiddenVisibilities()),
                alterPropertyVisibility.getVisibility(),
                property.getFetchHints()
            );
            graph.queueEvent(new AddPropertyEvent(graph, element.get(), newProperty));
        });
        mutation.getPropertySoftDeletes().forEach(propertySoftDeleteMutation ->
            graph.queueEvent(new SoftDeletePropertyEvent(graph, element.get(), propertySoftDeleteMutation))
        );
        mutation.getAdditionalVisibilities().forEach(additionalVisibilityAddMutation ->
            graph.queueEvent(new AddAdditionalVisibilityEvent(graph, element.get(), additionalVisibilityAddMutation))
        );
        mutation.getAdditionalVisibilityDeletes().forEach(additionalVisibilityDeleteMutation ->
            graph.queueEvent(new DeleteAdditionalVisibilityEvent(graph, element.get(), additionalVisibilityDeleteMutation))
        );
        mutation.getExtendedData().forEach(extendedDataMutation ->
            graph.queueEvent(new AddExtendedDataEvent(
                graph,
                element.get(),
                extendedDataMutation.getTableName(),
                extendedDataMutation.getRow(),
                extendedDataMutation.getColumnName(),
                extendedDataMutation.getKey(),
                extendedDataMutation.getValue(),
                extendedDataMutation.getVisibility()
            ))
        );
        mutation.getExtendedDataDeletes().forEach(extendedDataDeleteMutation ->
            graph.queueEvent(new DeleteExtendedDataEvent(
                graph,
                element.get(),
                extendedDataDeleteMutation.getTableName(),
                extendedDataDeleteMutation.getRow(),
                extendedDataDeleteMutation.getColumnName(),
                extendedDataDeleteMutation.getKey()
            ))
        );
        mutation.getAdditionalExtendedDataVisibilities().forEach(additionalExtendedDataVisibility ->
            graph.queueEvent(new AddAdditionalExtendedDataVisibilityEvent(
                graph,
                element.get(),
                additionalExtendedDataVisibility.getTableName(),
                additionalExtendedDataVisibility.getRow(),
                additionalExtendedDataVisibility.getAdditionalVisibility()
            ))
        );
        mutation.getAdditionalExtendedDataVisibilityDeletes().forEach(additionalExtendedDataVisibilityDelete ->
            graph.queueEvent(new DeleteAdditionalExtendedDataVisibilityEvent(
                graph,
                element.get(),
                additionalExtendedDataVisibilityDelete.getTableName(),
                additionalExtendedDataVisibilityDelete.getRow(),
                additionalExtendedDataVisibilityDelete.getAdditionalVisibility()
            ))
        );
        mutation.getMarkPropertyHiddenData().forEach(markPropertyHiddenData ->
            graph.queueEvent(new MarkHiddenPropertyEvent(
                graph,
                element.get(),
                markPropertyHiddenData.getKey(),
                markPropertyHiddenData.getName(),
                markPropertyHiddenData.getPropertyVisibility(),
                markPropertyHiddenData.getTimestamp(),
                markPropertyHiddenData.getVisibility(),
                markPropertyHiddenData.getEventData()
            ))
        );
        mutation.getMarkPropertyVisibleData().forEach(markPropertyVisibleData ->
            graph.queueEvent(new MarkVisiblePropertyEvent(
                graph,
                element.get(),
                markPropertyVisibleData.getKey(),
                markPropertyVisibleData.getName(),
                markPropertyVisibleData.getPropertyVisibility(),
                markPropertyVisibleData.getTimestamp(),
                markPropertyVisibleData.getVisibility(),
                markPropertyVisibleData.getEventData()
            ))
        );
        mutation.getMarkHiddenData().forEach(markHiddenData -> {
            if (element.get() instanceof Edge) {
                graph.queueEvent(new MarkHiddenEdgeEvent(graph, (Edge) element.get(), markHiddenData.getEventData()));
            } else if (element.get() instanceof Vertex) {
                graph.queueEvent(new MarkHiddenVertexEvent(graph, (Vertex) element.get(), markHiddenData.getEventData()));
            } else {
                throw new VertexiumException("Unexpected element type: " + element.get());
            }
        });
        mutation.getMarkVisibleData().forEach(markVisibleData -> {
            if (element.get() instanceof Edge) {
                graph.queueEvent(new MarkVisibleEdgeEvent(graph, (Edge) element.get(), markVisibleData.getEventData()));
            } else if (element.get() instanceof Vertex) {
                graph.queueEvent(new MarkVisibleVertexEvent(graph, (Vertex) element.get(), markVisibleData.getEventData()));
            } else {
                throw new VertexiumException("Unexpected element type: " + element.get());
            }
        });

        if (mutation.getSoftDeleteData() != null) {
            if (element.get() instanceof Edge) {
                graph.queueEvent(new SoftDeleteEdgeEvent(graph, (Edge) element.get(), mutation.getSoftDeleteData().getEventData()));
            } else if (element.get() instanceof Vertex) {
                graph.queueEvent(new SoftDeleteVertexEvent(graph, (Vertex) element.get(), mutation.getSoftDeleteData().getEventData()));
            } else {
                throw new VertexiumException("Unexpected element type: " + element.get());
            }
        }

        if (mutation.isDeleteElement()) {
            if (element.get() instanceof Edge) {
                graph.queueEvent(new DeleteEdgeEvent(graph, (Edge) element.get()));
            } else if (element.get() instanceof Vertex) {
                graph.queueEvent(new DeleteVertexEvent(graph, (Vertex) element.get()));
            } else {
                throw new VertexiumException("Unexpected element type: " + element.get());
            }
        }
    }
}
