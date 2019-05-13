package org.vertexium;

import com.google.common.collect.ImmutableSet;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.mutation.ExistingEdgeMutation;
import org.vertexium.query.QueryableIterable;

import java.util.Set;
import java.util.stream.Stream;

public class EdgeInfoEdge extends ElementBase implements Edge {
    private final Graph graph;
    private final String sourceVertexId;
    private final EdgeInfo edgeInfo;
    private final Authorizations authorizations;
    private final FetchHints fetchHints;

    public EdgeInfoEdge(
        Graph graph,
        String sourceVertexId,
        EdgeInfo edgeInfo,
        FetchHints fetchHints,
        Authorizations authorizations
    ) {
        this.graph = graph;
        this.sourceVertexId = sourceVertexId;
        this.edgeInfo = edgeInfo;
        this.fetchHints = fetchHints;
        this.authorizations = authorizations;
    }

    @Override
    public ElementType getElementType() {
        return ElementType.EDGE;
    }

    @Override
    public String getLabel() {
        return edgeInfo.getLabel();
    }

    @Override
    public String getVertexId(Direction direction) {
        if (direction == edgeInfo.getDirection()) {
            return sourceVertexId;
        }
        return edgeInfo.getVertexId();
    }

    @Override
    public String getOtherVertexId(String myVertexId) {
        if (sourceVertexId.equals(myVertexId)) {
            return edgeInfo.getVertexId();
        } else if (edgeInfo.getVertexId().equals(myVertexId)) {
            return sourceVertexId;
        }
        throw new VertexiumException("myVertexId(" + myVertexId + ") does not appear on edge (" + getId() + ").");
    }

    @Override
    public ExistingEdgeMutation prepareMutation() {
        return getEdge().prepareMutation();
    }

    private Edge getEdge() {
        return getGraph().getEdge(getId(), getFetchHints(), authorizations);
    }

    @Override
    public String getId() {
        return edgeInfo.getEdgeId();
    }

    @Override
    public Iterable<Property> getProperties() {
        return getEdge().getProperties();
    }

    @Override
    public Visibility getVisibility() {
        return getEdge().getVisibility();
    }

    @Override
    public long getTimestamp() {
        return getEdge().getTimestamp();
    }

    @Override
    public void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        getEdge().deleteProperty(key, name, visibility, authorizations);
    }

    @Override
    public void softDeleteProperty(String key, String name, Visibility visibility, Object eventData, Authorizations authorizations) {
        getEdge().softDeleteProperty(key, name, visibility, eventData, authorizations);
    }

    @Override
    public Graph getGraph() {
        return graph;
    }

    @Override
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Object data, Authorizations authorizations) {
        getEdge().markPropertyHidden(property, timestamp, visibility, data, authorizations);
    }

    @Override
    public void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Object eventData, Authorizations authorizations) {
        getEdge().markPropertyVisible(property, timestamp, visibility, eventData, authorizations);
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        return getEdge().getHiddenVisibilities();
    }

    @Override
    public ImmutableSet<String> getAdditionalVisibilities() {
        return getEdge().getAdditionalVisibilities();
    }

    @Override
    public ImmutableSet<String> getExtendedDataTableNames() {
        return getEdge().getExtendedDataTableNames();
    }

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName, FetchHints fetchHints) {
        return getEdge().getExtendedData(tableName, fetchHints);
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    @Override
    public Stream<HistoricalEvent> getHistoricalEvents(
        HistoricalEventId after,
        HistoricalEventsFetchHints fetchHints,
        User user
    ) {
        return getEdge().getHistoricalEvents(after, fetchHints, user);
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
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        return "EdgeInfoEdge{" +
            "edgeInfo=" + edgeInfo +
            '}';
    }
}
