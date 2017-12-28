package org.vertexium.event;

import org.vertexium.Element;
import org.vertexium.Graph;

import java.util.Objects;

public class DeleteExtendedDataEvent extends GraphEvent {
    private final Element element;
    private final String tableName;
    private final String row;
    private final String columnName;

    public DeleteExtendedDataEvent(
            Graph graph,
            Element element,
            String tableName,
            String row,
            String columnName
    ) {
        super(graph);
        this.element = element;
        this.tableName = tableName;
        this.row = row;
        this.columnName = columnName;
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

    public String getColumnName() {
        return columnName;
    }

    @Override
    public String toString() {
        return "DeleteExtendedDataEvent{" +
                "element=" + element +
                ", tableName='" + tableName + '\'' +
                ", row='" + row + '\'' +
                ", columnName='" + columnName + '\'' +
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
        DeleteExtendedDataEvent that = (DeleteExtendedDataEvent) o;
        return Objects.equals(element, that.element) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(row, that.row) &&
                Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(element, tableName, row, columnName);
    }
}
