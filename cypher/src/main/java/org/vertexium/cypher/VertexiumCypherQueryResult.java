package org.vertexium.cypher;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class VertexiumCypherQueryResult {
    private final String[] columnNames;
    private List<Row> rows;

    public VertexiumCypherQueryResult(String[] columnNames, List<Row> rows) {
        this.columnNames = columnNames;
        this.rows = rows;
    }

    public static VertexiumCypherQueryResult createEmpty() {
        return new VertexiumCypherQueryResult(new String[0], ImmutableList.of());
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public List<Row> getRows() {
        return rows;
    }

    public VertexiumCypherQueryResult concat(VertexiumCypherQueryResult newResults, boolean all) {
        List<Row> newRows = new ArrayList<>();
        newRows.addAll(getRows());
        if (all) {
            newRows.addAll(newResults.getRows());
        } else {
            for (Row row : newResults.getRows()) {
                if (!getRows().contains(row)) {
                    newRows.add(row);
                }
            }
        }
        return new VertexiumCypherQueryResult(columnNames, newRows);
    }

    public static class Row {
        private final List<Object> columns;

        public Row(List<Object> columns) {
            this.columns = columns;
        }

        public List<Object> getColumns() {
            return columns;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Row row = (Row) o;

            return columns.equals(row.columns);
        }

        @Override
        public int hashCode() {
            return columns.hashCode();
        }
    }
}
