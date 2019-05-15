package org.vertexium.inmemory;

import com.google.common.collect.ImmutableSet;
import org.vertexium.*;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.query.ExtendedDataQueryableIterable;
import org.vertexium.query.QueryableIterable;

import java.util.stream.Stream;

public abstract class InMemoryElement<TElement extends InMemoryElement> extends ElementBase {
    private final String id;
    private final FetchHints fetchHints;
    private InMemoryGraph graph;
    private InMemoryTableElement<TElement> inMemoryTableElement;
    private final Long endTime;
    private final User user;

    protected InMemoryElement(
        InMemoryGraph graph,
        String id,
        InMemoryTableElement<TElement> inMemoryTableElement,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        this.graph = graph;
        this.id = id;
        this.fetchHints = fetchHints;
        this.endTime = endTime;
        this.user = user;
        this.inMemoryTableElement = inMemoryTableElement;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public Visibility getVisibility() {
        return inMemoryTableElement.getVisibility();
    }

    @Override
    public long getTimestamp() {
        return inMemoryTableElement.getTimestamp();
    }

    @Override
    public User getUser() {
        return user;
    }

    protected void softDeleteProperty(String key, String name, Long timestamp, Visibility visibility, Object data, IndexHint indexHint, User user) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            getGraph().softDeleteProperty(inMemoryTableElement, property, timestamp, data, indexHint, user);
        }
    }

    protected void addAdditionalVisibility(
        String additionalVisibility,
        Object eventData,
        User user
    ) {
        getGraph().addAdditionalVisibility(inMemoryTableElement, additionalVisibility, eventData, user);
    }

    protected void deleteAdditionalVisibility(
        String additionalVisibility,
        Object eventData,
        User user
    ) {
        getGraph().deleteAdditionalVisibility(inMemoryTableElement, additionalVisibility, eventData, user);
    }

    private void addAdditionalExtendedDataVisibility(
        String tableName,
        String row,
        String additionalVisibility
    ) {
        getGraph().addAdditionalExtendedDataVisibility(
            this,
            tableName,
            row,
            additionalVisibility
        );
    }

    private void deleteAdditionalExtendedDataVisibility(
        String tableName,
        String row,
        String additionalVisibility
    ) {
        getGraph().deleteAdditionalExtendedDataVisibility(
            this,
            tableName,
            row,
            additionalVisibility
        );
    }

    private void deleteExtendedData(String tableName, String row, String columnName, String key, Visibility visibility) {
        getGraph().deleteExtendedData(this, tableName, row, columnName, key, visibility, user);
    }

    protected void extendedData(ExtendedDataMutation extendedData, User user) {
        ExtendedDataRowId extendedDataRowId = new ExtendedDataRowId(
            ElementType.getTypeFromElement(this),
            getId(),
            extendedData.getTableName(),
            extendedData.getRow()
        );
        getGraph().extendedData(this, extendedDataRowId, extendedData, user);
    }

    @Override
    public Object getPropertyValue(String name) {
        Property p = getProperty(name);
        return p == null ? null : p.getValue();
    }

    @Override
    public Object getPropertyValue(String key, String name) {
        Property p = getProperty(key, name);
        return p == null ? null : p.getValue();
    }

    public void addPropertyValue(
        String key,
        String name,
        Object value,
        Metadata metadata,
        Visibility visibility,
        Long timestamp,
        boolean indexAfterAdd,
        User user
    ) {
        getGraph().addPropertyValue(this, inMemoryTableElement, key, name, value, metadata, visibility, timestamp, user);
        if (indexAfterAdd) {
            getGraph().getSearchIndex().addElement(getGraph(), this, null, null, user);
        }
    }

    @Override
    public Iterable<Property> getProperties() {
        if (!getFetchHints().isIncludeProperties()) {
            throw new VertexiumMissingFetchHintException(getFetchHints(), "includeProperties");
        }
        return inMemoryTableElement.getProperties(fetchHints, endTime, user);
    }

    @Override
    public Stream<HistoricalEvent> getHistoricalEvents(
        HistoricalEventId after,
        HistoricalEventsFetchHints historicalEventsFetchHints,
        User user
    ) {
        return inMemoryTableElement.getHistoricalEvents(getGraph(), after, historicalEventsFetchHints, user);
    }

    @Override
    public abstract <T extends Element> ExistingElementMutation<T> prepareMutation();

    @Override
    public InMemoryGraph getGraph() {
        return this.graph;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Element) {
            Element objElem = (Element) obj;
            return getId().equals(objElem.getId());
        }
        return super.equals(obj);
    }

    @Override
    public String toString() {
        if (this instanceof Edge) {
            Edge edge = (Edge) this;
            return getId() + ":[" + edge.getVertexId(Direction.OUT) + "-" + edge.getLabel() + "->" + edge.getVertexId(Direction.IN) + "]";
        }
        return getId();
    }

    public boolean canRead(User user) {
        return inMemoryTableElement.canRead(getFetchHints(), user);
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        return inMemoryTableElement.getHiddenVisibilities();
    }

    @Override
    public ImmutableSet<String> getAdditionalVisibilities() {
        return inMemoryTableElement.getAdditionalVisibilities();
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    InMemoryTableElement<TElement> getInMemoryTableElement() {
        return inMemoryTableElement;
    }

    void setInMemoryTableElement(InMemoryTableElement<TElement> inMemoryTableElement) {
        this.inMemoryTableElement = inMemoryTableElement;
    }

    @Override
    public ImmutableSet<String> getExtendedDataTableNames() {
        if (!getFetchHints().isIncludeExtendedDataTableNames()) {
            throw new VertexiumMissingFetchHintException(getFetchHints(), "includeExtendedDataTableNames");
        }
        return graph.getExtendedDataTableNames(ElementType.getTypeFromElement(this), id, getFetchHints(), user);
    }

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName, FetchHints fetchHints) {
        return new ExtendedDataQueryableIterable(
            getGraph(),
            this,
            tableName,
            graph.getExtendedDataTable(ElementType.getTypeFromElement(this), id, tableName, fetchHints, user)
        );
    }
}
