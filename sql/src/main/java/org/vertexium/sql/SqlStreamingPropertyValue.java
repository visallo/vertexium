package org.vertexium.sql;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.vertexium.VertexiumException;
import org.vertexium.Visibility;
import org.vertexium.property.DefaultStreamingPropertyValue;
import org.vertexium.util.AutoDeleteFileInputStream;
import org.vertexium.util.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.vertexium.sql.SqlStreamingPropertyTable.*;

public class SqlStreamingPropertyValue extends DefaultStreamingPropertyValue {
    private static final int IN_MEMORY_STREAM_MAX_BYTES = 1024 * 1024;
    private static final long serialVersionUID = 3515084569014897749L;

    private final DBI dbi;
    private final String tableName;
    private final String elementId;
    private final String key;
    private final String name;
    private final Visibility visibility;
    private final long timestamp;

    public SqlStreamingPropertyValue(
            Class valueType, long length, DBI dbi, String tableName, String elementId,
            String key, String name, Visibility visibility, long timestamp
    ) {
        super(null, valueType, length);
        this.dbi = dbi;
        this.tableName = tableName;
        this.elementId = elementId;
        this.key = key;
        this.name = name;
        this.visibility = visibility;
        this.timestamp = timestamp;
    }

    @Override
    public InputStream getInputStream() {
        try (Handle handle = dbi.open()) {
            Row row = handle
                    .createQuery(String.format(
                            "select %s from %s where %s = ?",
                            VALUE_COLUMN_NAME, tableName, KEY_COLUMN_NAME
                    ))
                    .bind(0, makeId(elementId, key, name, visibility, timestamp))
                    .map(new RowResultSetMapper()).first();

            return copyInputStream(row.inputStream);
        }
    }

    private InputStream copyInputStream(InputStream inputStream) {
        try {
            Long length = getLength();
            if (length == null || length < 0 || length > IN_MEMORY_STREAM_MAX_BYTES) {
                return new AutoDeleteFileInputStream(inputStream);
            } else {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream((int) (long) length);
                IOUtils.copy(inputStream, outputStream);
                return new ByteArrayInputStream(outputStream.toByteArray());
            }
        } catch (IOException ex) {
            throw new VertexiumException(ex);
        }
    }

    private static class RowResultSetMapper implements ResultSetMapper<Row> {
        public Row map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
            Row row = new Row();
            row.inputStream = rs.getBinaryStream(VALUE_COLUMN_NAME);
            return row;
        }
    }
}
