package org.vertexium.sql;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.IntegerMapper;
import org.vertexium.VertexiumException;
import org.vertexium.Visibility;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.util.AutoDeleteFileInputStream;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

class SqlStreamingPropertyTable {
    protected final String tableName;
    protected static final String KEY_COLUMN_NAME = "id";
    protected static final String VALUE_COLUMN_NAME = "data";
    protected static final String VALUE_TYPE_COLUMN_NAME = "type";
    protected static final String VALUE_LENGTH_COLUMN_NAME = "length";
    private final DBI dbi;

    SqlStreamingPropertyTable(String tableName, DataSource dataSource) {
        this.tableName = tableName;
        this.dbi = new DBI(dataSource);
    }

    public StreamingPropertyValueRef put(
            String elementId,
            String key,
            String name,
            Visibility visibility,
            long timestamp,
            StreamingPropertyValue value
    ) {
        StreamAndLength streamAndLength = streamAndLength(value.getInputStream(), value.getLength());
        String id = makeId(elementId, key, name, visibility, timestamp);
        try (Handle handle = dbi.open()) {
            int count = handle.createQuery(String.format(
                    "select count(*) from %s where %s = ?", tableName, KEY_COLUMN_NAME))
                    .bind(0, id)
                    .map(IntegerMapper.FIRST)
                    .first();

            if (count == 0) {
                handle.execute(String.format(
                        "insert into %s (%s, %s, %s, %s) values (?, ?, ?, ?)",
                        tableName, KEY_COLUMN_NAME, VALUE_COLUMN_NAME, VALUE_TYPE_COLUMN_NAME,
                        VALUE_LENGTH_COLUMN_NAME
                               ),
                               id, streamAndLength.inputStream, value.getValueType().getName(), streamAndLength.length
                );
            } else {
                handle.execute(String.format(
                        "update %s set %s = ?, %s = ? where %s = ?",
                        tableName, VALUE_COLUMN_NAME, VALUE_LENGTH_COLUMN_NAME, KEY_COLUMN_NAME
                               ),
                               streamAndLength.inputStream, streamAndLength.length, id
                );
            }
            return new SqlStreamingPropertyValueRef(value, elementId, key, name, visibility, timestamp);
        } finally {
            try {
                streamAndLength.inputStream.close();
            } catch (IOException ex) {
                throw new VertexiumException(ex);
            }
        }
    }

    public StreamingPropertyValue get(
            String elementId,
            String key,
            String name,
            Visibility visibility,
            long timestamp
    ) {
        try (Handle handle = dbi.open()) {
            Row row = handle
                    .createQuery(String.format(
                            "select %s, %s from %s where %s = ?",
                            VALUE_TYPE_COLUMN_NAME, VALUE_LENGTH_COLUMN_NAME, tableName, KEY_COLUMN_NAME
                    ))
                    .bind(0, makeId(elementId, key, name, visibility, timestamp))
                    .map(new RowResultSetMapper()).first();
            if (row == null) {
                return null;
            }

            return new SqlStreamingPropertyValue(
                    row.valueType,
                    row.length,
                    dbi,
                    tableName,
                    elementId, key,
                    name,
                    visibility,
                    timestamp
            );
        }
    }

    private static StreamAndLength streamAndLength(InputStream inputStream, Long length) {
        if (length == null || length < 0) {
            try {
                AutoDeleteFileInputStream fileInputStream = new AutoDeleteFileInputStream(inputStream);
                return new StreamAndLength(fileInputStream, fileInputStream.getFileLength());
            } catch (IOException ex) {
                throw new VertexiumException(ex);
            }
        } else {
            return new StreamAndLength(inputStream, length);
        }
    }

    static String makeId(String elementId, String key, String name, Visibility visibility, long timestamp) {
        return String.format("%s:%s:%s:%s:%d", elementId, key, name, visibility.getVisibilityString(), timestamp);
    }

    static final class Row {
        InputStream inputStream;
        Class valueType;
        long length;
    }

    private static final class RowResultSetMapper implements ResultSetMapper<Row> {
        public Row map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
            try {
                Row row = new Row();
                row.valueType = Class.forName(rs.getString(VALUE_TYPE_COLUMN_NAME));
                row.length = rs.getLong(VALUE_LENGTH_COLUMN_NAME);
                return row;
            } catch (ClassNotFoundException ex) {
                throw new VertexiumException(ex);
            }
        }
    }

    private static final class StreamAndLength {
        final InputStream inputStream;
        final long length;

        StreamAndLength(InputStream inputStream, long length) {
            this.inputStream = inputStream;
            this.length = length;
        }
    }
}
