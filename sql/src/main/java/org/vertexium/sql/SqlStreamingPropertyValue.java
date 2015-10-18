package org.vertexium.sql;

import com.google.common.base.Throwables;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.vertexium.Visibility;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.util.AutoDeleteFileInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.vertexium.sql.SqlStreamingPropertyTable.*;

public class SqlStreamingPropertyValue extends StreamingPropertyValue {
    private DBI dbi;
    private String tableName;
    private String elementId;
    private String key;
    private String name;
    private Visibility visibility;
    private long timestamp;

    public SqlStreamingPropertyValue(Class valueType, long length, DBI dbi, String tableName, String elementId,
                                     String key, String name, Visibility visibility, long timestamp) {
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
                            VALUE_COLUMN_NAME, tableName, KEY_COLUMN_NAME))
                    .bind(0, makeId(elementId, key, name, visibility, timestamp))
                    .map(new RowResultSetMapper()).first();

            return new AutoDeleteFileInputStream(row.inputStream);
        } catch (IOException ex) {
            throw Throwables.propagate(ex);
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
