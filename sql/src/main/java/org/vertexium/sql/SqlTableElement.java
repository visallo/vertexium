package org.vertexium.sql;

import org.vertexium.Authorizations;
import org.vertexium.Metadata;
import org.vertexium.Property;
import org.vertexium.Visibility;
import org.vertexium.inmemory.InMemoryElement;
import org.vertexium.inmemory.InMemoryTableElement;
import org.vertexium.inmemory.mutations.Mutation;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.sql.collections.Storable;

import java.util.Map;

public abstract class SqlTableElement<TElement extends InMemoryElement>
        extends InMemoryTableElement<TElement> implements Storable<SqlTableElement<TElement>, SqlGraph> {

    private transient Map<String, SqlTableElement<TElement>> container;
    private transient SqlGraph graph;

    @Override
    public void setContainer(Map<String, SqlTableElement<TElement>> container, SqlGraph graph) {
        this.container = container;
        this.graph = graph;
    }

    @Override
    public void store() {
        container.put(getId(), this);
    }

    protected SqlTableElement(String id) {
        super(id);
    }

    @Override
    public void addAll(Mutation... newMutations) {
        super.addAll(newMutations);
        store();
    }

    @Override
    protected void deleteProperty(Property p) {
        super.deleteProperty(p);
        store();
    }

    @Override
    public void appendSoftDeleteMutation(Long timestamp) {
        super.appendSoftDeleteMutation(timestamp);
        store();
    }

    @Override
    public void appendMarkHiddenMutation(Visibility visibility) {
        super.appendMarkHiddenMutation(visibility);
        store();
    }

    @Override
    public void appendMarkVisibleMutation(Visibility visibility) {
        super.appendMarkVisibleMutation(visibility);
        store();
    }

    @Override
    public Property appendMarkPropertyHiddenMutation(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        Property prop = super.appendMarkPropertyHiddenMutation(key, name, propertyVisibility, timestamp, visibility, authorizations);
        store();
        return prop;
    }

    @Override
    public Property appendMarkPropertyVisibleMutation(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        Property prop = super.appendMarkPropertyVisibleMutation(key, name, propertyVisibility, timestamp, visibility, authorizations);
        store();
        return prop;
    }

    @Override
    public void appendSoftDeletePropertyMutation(String key, String name, Visibility propertyVisibility, Long timestamp) {
        super.appendSoftDeletePropertyMutation(key, name, propertyVisibility, timestamp);
        store();
    }

    @Override
    public void appendAlterVisibilityMutation(Visibility newVisibility) {
        super.appendAlterVisibilityMutation(newVisibility);
        store();
    }

    @Override
    public void appendAddPropertyMutation(String key, String name, Object value, Metadata metadata, Visibility visibility, Long timestamp) {
        super.appendAddPropertyMutation(key, name, value, metadata, visibility, timestamp);
        store();
    }

    @Override
    public void appendAlterEdgeLabelMutation(String newEdgeLabel) {
        super.appendAlterEdgeLabelMutation(newEdgeLabel);
        store();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected StreamingPropertyValue loadStreamingPropertyValue(StreamingPropertyValueRef<?> streamingPropertyValueRef) {
        return ((StreamingPropertyValueRef<SqlGraph>) streamingPropertyValueRef).toStreamingPropertyValue(graph);
    }
}
