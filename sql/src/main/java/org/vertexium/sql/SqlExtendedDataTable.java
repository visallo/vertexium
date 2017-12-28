package org.vertexium.sql;

import com.google.common.collect.ImmutableSet;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.vertexium.*;
import org.vertexium.inmemory.InMemoryExtendedDataRow;
import org.vertexium.inmemory.InMemoryExtendedDataTable;
import org.vertexium.security.ColumnVisibility;
import org.vertexium.security.VisibilityEvaluator;
import org.vertexium.security.VisibilityParseException;
import org.vertexium.util.GroupingIterable;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SqlExtendedDataTable extends InMemoryExtendedDataTable {
    private final String tableName;
    protected static final String ELEMENT_TYPE_COLUMN_NAME = "type";
    protected static final String ELEMENT_ID_COLUMN_NAME = "elementId";
    protected static final String TABLE_NAME_COLUMN_NAME = "tableName";
    protected static final String ROW_ID_COLUMN_NAME = "rowId";
    protected static final String COLUMN_COLUMN_NAME = "column";
    protected static final String VALUE_COLUMN_NAME = "value";
    protected static final String TIMESTAMP_COLUMN_NAME = "timestamp";
    protected static final String VISIBILITY_COLUMN_NAME = "visibility";
    private final DBI dbi;
    private final VertexiumSerializer serializer;

    public SqlExtendedDataTable(String tableName, DataSource dataSource, VertexiumSerializer serializer) {
        this.tableName = tableName;
        this.dbi = new DBI(dataSource);
        this.serializer = serializer;
    }

    @Override
    public ImmutableSet<String> getTableNames(ElementType elementType, String elementId, Authorizations authorizations) {
        try (Handle handle = dbi.open()) {
            Query<String> rows = handle
                    .createQuery(
                            String.format(
                                    "select distinct %s from %s where %s = ? AND %s = ?",
                                    TABLE_NAME_COLUMN_NAME,
                                    tableName,
                                    ELEMENT_TYPE_COLUMN_NAME, ELEMENT_ID_COLUMN_NAME
                            )
                    )
                    .bind(0, elementType.name())
                    .bind(1, elementId)
                    .map(new TableNameResultSetMapper());

            ImmutableSet.Builder<String> result = ImmutableSet.builder();
            for (String tableName : rows) {
                result.add(tableName);
            }
            return result.build();
        }
    }

    @Override
    public Iterable<? extends ExtendedDataRow> getTable(ElementType elementType, String elementId, String tableName, Authorizations authorizations) {
        VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(new org.vertexium.security.Authorizations(authorizations.getAuthorizations()));

        Handle handle = dbi.open();
        Query<Row> rows = handle
                .createQuery(
                        String.format(
                                "select %s, %s, %s, %s, %s from %s where %s = ? AND %s = ? AND %s = ?",
                                ROW_ID_COLUMN_NAME, COLUMN_COLUMN_NAME, VALUE_COLUMN_NAME, TIMESTAMP_COLUMN_NAME, VISIBILITY_COLUMN_NAME,
                                this.tableName,
                                ELEMENT_TYPE_COLUMN_NAME, ELEMENT_ID_COLUMN_NAME, TABLE_NAME_COLUMN_NAME
                        )
                )
                .bind(0, elementType.name())
                .bind(1, elementId)
                .bind(2, tableName)
                .map(new RowResultSetMapper());
        return new GroupingIterable<Row, InMemoryExtendedDataRow>(rows) {
            @Override
            protected boolean isIncluded(Row item) {
                try {
                    return visibilityEvaluator.evaluate(new ColumnVisibility(item.visibility.getVisibilityString()));
                } catch (VisibilityParseException e) {
                    throw new VertexiumException("Could not parse visibility: " + item.visibility);
                }
            }

            @Override
            protected InMemoryExtendedDataRow createGroup(Row row) {
                ExtendedDataRowId id = createExtendedDataRowId(row);
                InMemoryExtendedDataRow result = new InMemoryExtendedDataRow(id);
                addToGroup(result, row);
                return result;
            }

            @Override
            protected boolean isPartOfGroup(InMemoryExtendedDataRow extendedDataRow, Row row) {
                return createExtendedDataRowId(row).equals(extendedDataRow.getId());
            }

            private ExtendedDataRowId createExtendedDataRowId(Row row) {
                return new ExtendedDataRowId(elementType, elementId, tableName, row.rowId);
            }

            @Override
            protected void addToGroup(InMemoryExtendedDataRow extendedDataRow, Row row) {
                Object value = serializer.bytesToObject(row.value);
                extendedDataRow.addColumn(row.column, value, row.timestamp, row.visibility);
            }

            @Override
            public void close() {
                handle.close();
            }
        };
    }

    @Override
    public void addData(ExtendedDataRowId rowId, String column, Object value, long timestamp, Visibility visibility) {
        try (Handle handle = dbi.open()) {
            handle.execute(
                    String.format(
                            "insert into %s (%s, %s, %s, %s, %s, %s, %s, %s) values (?, ?, ?, ?, ?, ?, ?, ?)",
                            tableName,
                            ELEMENT_TYPE_COLUMN_NAME,
                            ELEMENT_ID_COLUMN_NAME,
                            TABLE_NAME_COLUMN_NAME,
                            ROW_ID_COLUMN_NAME,
                            COLUMN_COLUMN_NAME,
                            VALUE_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME,
                            VISIBILITY_COLUMN_NAME
                    ),
                    rowId.getElementType().name(),
                    rowId.getElementId(),
                    rowId.getTableName(),
                    rowId.getRowId(),
                    column,
                    serializer.objectToBytes(value),
                    timestamp,
                    visibility.getVisibilityString()
            );
        }
    }

    @Override
    public void remove(ExtendedDataRowId rowId) {
        try (Handle handle = dbi.open()) {
            handle.execute(
                    String.format(
                            "delete from %s where %s=? AND %s=? AND %s=? AND %s=?",
                            tableName,
                            ELEMENT_TYPE_COLUMN_NAME,
                            ELEMENT_ID_COLUMN_NAME,
                            TABLE_NAME_COLUMN_NAME,
                            ROW_ID_COLUMN_NAME
                    ),
                    rowId.getElementType().name(),
                    rowId.getElementId(),
                    rowId.getTableName(),
                    rowId.getRowId()
            );
        }
    }

    @Override
    public void removeColumn(ExtendedDataRowId rowId, String columnName, Visibility visibility) {
        try (Handle handle = dbi.open()) {
            handle.execute(
                    String.format(
                            "delete from %s where %s=? AND %s=? AND %s=? AND %s=? AND %s=? AND %s=?",
                            tableName,
                            ELEMENT_TYPE_COLUMN_NAME,
                            ELEMENT_ID_COLUMN_NAME,
                            TABLE_NAME_COLUMN_NAME,
                            ROW_ID_COLUMN_NAME,
                            COLUMN_COLUMN_NAME,
                            VISIBILITY_COLUMN_NAME
                    ),
                    rowId.getElementType().name(),
                    rowId.getElementId(),
                    rowId.getTableName(),
                    rowId.getRowId(),
                    columnName,
                    visibility.getVisibilityString()
            );
        }
    }

    static final class Row {
        public String rowId;
        public String column;
        public long timestamp;
        public Visibility visibility;
        public byte[] value;
    }

    private static final class RowResultSetMapper implements ResultSetMapper<Row> {
        public Row map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
            Row row = new Row();
            row.rowId = rs.getString(ROW_ID_COLUMN_NAME);
            row.column = rs.getString(COLUMN_COLUMN_NAME);
            row.value = rs.getBytes(VALUE_COLUMN_NAME);
            row.timestamp = rs.getLong(TIMESTAMP_COLUMN_NAME);
            row.visibility = new Visibility(rs.getString(VISIBILITY_COLUMN_NAME));
            return row;
        }
    }

    private static final class TableNameResultSetMapper implements ResultSetMapper<String> {
        @Override
        public String map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
            return rs.getString(TABLE_NAME_COLUMN_NAME);
        }
    }
}

