package org.vertexium.inmemory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.vertexium.*;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.util.IterableUtils;
import org.vertexium.util.StreamUtils;

import java.util.*;
import java.util.stream.Collectors;

public class MapInMemoryExtendedDataTable extends InMemoryExtendedDataTable {
    private Map<ElementType, ElementTypeData> elementTypeData = new HashMap<>();

    @Override
    public ImmutableSet<String> getTableNames(
        ElementType elementType,
        String elementId,
        FetchHints fetchHints,
        Authorizations authorizations
    ) {
        ElementTypeData data = elementTypeData.get(elementType);
        if (data == null) {
            return ImmutableSet.of();
        }
        return data.getTableNames(elementId, fetchHints, authorizations);
    }

    @Override
    public Iterable<ExtendedDataRow> getTable(
        ElementType elementType,
        String elementId,
        String tableName,
        FetchHints fetchHints,
        Authorizations authorizations
    ) {
        ElementTypeData data = elementTypeData.get(elementType);
        if (data == null) {
            return ImmutableList.of();
        }
        return data.getTable(elementId, tableName, fetchHints, authorizations);
    }

    @Override
    public synchronized void addData(
        ExtendedDataRowId rowId,
        String column,
        String key,
        Object value,
        long timestamp,
        Visibility visibility
    ) {
        ElementTypeData data = elementTypeData.computeIfAbsent(rowId.getElementType(), k -> new ElementTypeData());
        data.addData(rowId, column, key, value, timestamp, visibility);
    }

    @Override
    public void remove(ExtendedDataRowId rowId) {
        ElementTypeData data = elementTypeData.get(rowId.getElementType());
        if (data != null) {
            data.removeData(rowId);
        }
    }

    @Override
    public void removeColumn(ExtendedDataRowId rowId, String columnName, String key, Visibility visibility) {
        ElementTypeData data = elementTypeData.get(rowId.getElementType());
        if (data != null) {
            data.removeColumn(rowId, columnName, key, visibility);
        }
    }

    @Override
    public void addAdditionalVisibility(
        ExtendedDataRowId rowId,
        String additionalVisibility
    ) {
        ElementTypeData data = elementTypeData.computeIfAbsent(rowId.getElementType(), k -> new ElementTypeData());
        data.addAdditionalVisibility(rowId, additionalVisibility);
    }

    @Override
    public void deleteAdditionalVisibility(
        ExtendedDataRowId rowId,
        String additionalVisibility
    ) {
        ElementTypeData data = elementTypeData.computeIfAbsent(rowId.getElementType(), k -> new ElementTypeData());
        data.deleteAdditionalVisibility(rowId, additionalVisibility);
    }

    private static class ElementTypeData {
        Map<String, ElementData> elementData = new HashMap<>();

        public ImmutableSet<String> getTableNames(String elementId, FetchHints fetchHints, Authorizations authorizations) {
            ElementData data = elementData.get(elementId);
            if (data == null) {
                return ImmutableSet.of();
            }
            return data.getTableNames(fetchHints, authorizations);
        }

        public Iterable<ExtendedDataRow> getTable(
            String elementId,
            String tableName,
            FetchHints fetchHints,
            Authorizations authorizations
        ) {
            ElementData data = elementData.get(elementId);
            if (data == null) {
                return ImmutableList.of();
            }
            return data.getTable(tableName, fetchHints, authorizations);
        }

        public synchronized void addData(
            ExtendedDataRowId rowId,
            String column,
            String key,
            Object value,
            long timestamp,
            Visibility visibility
        ) {
            ElementData data = elementData.computeIfAbsent(rowId.getElementId(), k -> new ElementData());
            data.addData(rowId, column, key, value, timestamp, visibility);
        }

        public void removeData(ExtendedDataRowId rowId) {
            ElementData data = elementData.get(rowId.getElementId());
            if (data != null) {
                data.removeData(rowId);
            }
        }

        public void removeColumn(ExtendedDataRowId rowId, String columnName, String key, Visibility visibility) {
            ElementData data = elementData.get(rowId.getElementId());
            if (data != null) {
                data.removeColumn(rowId, columnName, key, visibility);
            }
        }

        public void addAdditionalVisibility(ExtendedDataRowId rowId, String additionalVisibility) {
            ElementData data = elementData.get(rowId.getElementId());
            if (data != null) {
                data.addAdditionalVisibility(rowId, additionalVisibility);
            }
        }

        public void deleteAdditionalVisibility(ExtendedDataRowId rowId, String additionalVisibility) {
            ElementData data = elementData.get(rowId.getElementId());
            if (data != null) {
                data.deleteAdditionalVisibility(rowId, additionalVisibility);
            }
        }
    }

    private static class ElementData {
        private final Map<String, Table> tables = new HashMap<>();

        public ImmutableSet<String> getTableNames(FetchHints fetchHints, Authorizations authorizations) {
            VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(new org.vertexium.security.Authorizations(authorizations.getAuthorizations()));
            return tables.entrySet().stream()
                .filter(entry -> entry.getValue().canRead(visibilityEvaluator, fetchHints))
                .map(Map.Entry::getKey)
                .collect(StreamUtils.toImmutableSet());
        }

        public Iterable<ExtendedDataRow> getTable(String tableName, FetchHints fetchHints, Authorizations authorizations) {
            VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(new org.vertexium.security.Authorizations(authorizations.getAuthorizations()));
            Table table = tables.get(tableName);
            if (table == null) {
                throw new VertexiumException("Invalid table '" + tableName + "'");
            }
            Iterable<ExtendedDataRow> rows = table.getRows(visibilityEvaluator, fetchHints);
            if (!rows.iterator().hasNext()) {
                return Collections.emptyList();
            }
            return rows;
        }

        public synchronized void addData(
            ExtendedDataRowId rowId,
            String column,
            String key,
            Object value,
            long timestamp,
            Visibility visibility
        ) {
            Table table = tables.computeIfAbsent(rowId.getTableName(), k -> new Table());
            table.addData(rowId, column, key, value, timestamp, visibility);
        }

        public void removeData(ExtendedDataRowId rowId) {
            Table table = tables.get(rowId.getTableName());
            if (table != null) {
                table.removeData(rowId);
            }
        }

        public void removeColumn(ExtendedDataRowId rowId, String columnName, String key, Visibility visibility) {
            Table table = tables.get(rowId.getTableName());
            if (table != null) {
                table.removeColumn(rowId, columnName, key, visibility);
            }
        }

        public void addAdditionalVisibility(ExtendedDataRowId rowId, String additionalVisibility) {
            Table table = tables.get(rowId.getTableName());
            if (table != null) {
                table.addAdditionalVisibility(rowId, additionalVisibility);
            }
        }

        public void deleteAdditionalVisibility(ExtendedDataRowId rowId, String additionalVisibility) {
            Table table = tables.get(rowId.getTableName());
            if (table != null) {
                table.deleteAdditionalVisibility(rowId, additionalVisibility);
            }
        }

        private class Table {
            private final TreeSet<InMemoryExtendedDataRow> rows = new TreeSet<>();

            public Iterable<ExtendedDataRow> getRows(VisibilityEvaluator visibilityEvaluator, FetchHints fetchHints) {
                return rows.stream()
                    .map(row -> row.toReadable(visibilityEvaluator, fetchHints))
                    .filter(Objects::nonNull)
                    .filter(row -> IterableUtils.count(row.getProperties()) > 0)
                    .collect(Collectors.toList());
            }

            public boolean canRead(VisibilityEvaluator visibilityEvaluator, FetchHints fetchHints) {
                return rows.stream().anyMatch(r -> r.canRead(visibilityEvaluator, fetchHints));
            }

            public void addData(
                ExtendedDataRowId rowId,
                String column,
                String key,
                Object value,
                long timestamp,
                Visibility visibility
            ) {
                InMemoryExtendedDataRow row = findOrAddRow(rowId);
                row.addColumn(column, key, value, timestamp, visibility);
            }

            private InMemoryExtendedDataRow findOrAddRow(ExtendedDataRowId rowId) {
                InMemoryExtendedDataRow row = findRow(rowId);
                if (row != null) {
                    return row;
                }
                row = new InMemoryExtendedDataRow(rowId, FetchHints.ALL);
                rows.add(row);
                return row;
            }

            private InMemoryExtendedDataRow findRow(ExtendedDataRowId rowId) {
                for (InMemoryExtendedDataRow row : rows) {
                    if (row.getId().equals(rowId)) {
                        return row;
                    }
                }
                return null;
            }

            public void removeData(ExtendedDataRowId rowId) {
                rows.removeIf(row -> row.getId().equals(rowId));
            }

            public void removeColumn(ExtendedDataRowId rowId, String columnName, String key, Visibility visibility) {
                InMemoryExtendedDataRow row = findRow(rowId);
                if (row == null) {
                    return;
                }
                row.removeColumn(columnName, key, visibility);
            }

            public void addAdditionalVisibility(ExtendedDataRowId rowId, String additionalVisibility) {
                InMemoryExtendedDataRow row = findRow(rowId);
                if (row == null) {
                    return;
                }
                row.addAdditionalVisibility(additionalVisibility);
            }

            public void deleteAdditionalVisibility(ExtendedDataRowId rowId, String additionalVisibility) {
                InMemoryExtendedDataRow row = findRow(rowId);
                if (row == null) {
                    return;
                }
                row.deleteAdditionalVisibility(additionalVisibility);
            }
        }
    }

}
