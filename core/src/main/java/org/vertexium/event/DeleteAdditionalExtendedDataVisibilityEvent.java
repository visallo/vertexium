package org.vertexium.event;

import org.vertexium.Element;
import org.vertexium.Graph;

public class DeleteAdditionalExtendedDataVisibilityEvent extends GraphEvent {
    private final Element element;
    private final String tableName;
    private final String row;
    private final String additionalVisibility;

    public DeleteAdditionalExtendedDataVisibilityEvent(
        Graph graph,
        Element element,
        String tableName,
        String row,
        String additionalVisibility
    ) {
        super(graph);
        this.element = element;
        this.tableName = tableName;
        this.row = row;
        this.additionalVisibility = additionalVisibility;
    }

    public Element getElement() {
        return element;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRow() {
        return row;
    }

    public String getAdditionalVisibility() {
        return additionalVisibility;
    }
}
