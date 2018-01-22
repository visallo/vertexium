package org.vertexium.event;

import org.vertexium.ExtendedDataRowId;
import org.vertexium.Graph;

public class DeleteExtendedDataRowEvent extends GraphEvent {
    private final ExtendedDataRowId id;

    public DeleteExtendedDataRowEvent(Graph graph, ExtendedDataRowId id) {
        super(graph);
        this.id = id;
    }

    public ExtendedDataRowId getId() {
        return id;
    }

    @Override
    public String toString() {
        return "DeleteExtendedDataRowEvent{" +
                "id=" + id +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DeleteExtendedDataRowEvent that = (DeleteExtendedDataRowEvent) o;

        if (!id.equals(that.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
