package org.vertexium.sql.collections;

import org.skife.jdbi.v2.*;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.IntegerMapper;
import org.skife.jdbi.v2.util.StringMapper;
import org.vertexium.VertexiumSerializer;
import org.vertexium.util.CloseableUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@SuppressWarnings("NullableProblems")
public class SqlMap<T> extends AbstractMap<String, T> {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(SqlMap.class);

    protected final String tableName;
    protected final String keyColumnName;
    protected final String valueColumnName;
    private final DBI dbi;
    private final VertexiumSerializer serializer;
    private final ResultSetMapper<MapEntry<byte[]>> entrySetMapper;
    private Object storableContext;

    public SqlMap(String tableName, String keyColumnName, String valueColumnName, DataSource dataSource,
                  VertexiumSerializer serializer) {
        this.tableName = tableName;
        this.keyColumnName = keyColumnName;
        this.valueColumnName = valueColumnName;
        this.dbi = new DBI(dataSource);
        this.serializer = serializer;
        this.entrySetMapper = new ResultSetMapper<MapEntry<byte[]>>() {
            public MapEntry<byte[]> map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
                String key = rs.getString(SqlMap.this.keyColumnName);
                byte[] value = rs.getBytes(SqlMap.this.valueColumnName);
                return new MapEntry<>(key, value);
            }
        };
    }

    public void setStorableContext(Object storableContext) {
        this.storableContext = storableContext;
    }

    @Override
    public Set<Entry<String, T>> entrySet() {
        final Handle handle = dbi.open();
        final Query<MapEntry<byte[]>> query = handle
                .createQuery(String.format(
                        "select %s, %s from %s order by %s", keyColumnName, valueColumnName, tableName, keyColumnName))
                .map(entrySetMapper);

        return new IteratingSet<Entry<String, T>>() {
            @Override
            public Iterator<Entry<String, T>> createIterator() {
                return new QueryResultIterator<Entry<String, T>, MapEntry<byte[]>>(query, handle) {
                    @Override
                    public Entry<String, T> next() {
                        MapEntry<byte[]> stringifiedEntry = resultIterator.next();
                        String key = stringifiedEntry.getKey();
                        T value = withContainer(serializer.<T>bytesToObject(stringifiedEntry.getValue()));
                        return new MapEntry<>(key, value);
                    }
                };
            }
        };
    }

    @Override
    public Set<String> keySet() {
        final Handle handle = dbi.open();
        final Query<String> query = handle
                .createQuery(String.format("select %s from %s order by %s", keyColumnName, tableName, keyColumnName))
                .map(StringMapper.FIRST);

        return new IteratingSet<String>() {
            @Override
            public Iterator<String> createIterator() {
                return new QueryResultIterator<>(query, handle);
            }
        };
    }

    @Override
    public Collection<T> values() {
        final Handle handle = dbi.open();
        final Query<byte[]> query = handle
                .createQuery(String.format("select %s from %s order by %s", valueColumnName, tableName, keyColumnName))
                .map(ByteArrayMapper.FIRST);

        return new IteratingSet<T>() {
            @Override
            public Iterator<T> createIterator() {
                return new QueryResultIterator<T, byte[]>(query, handle) {
                    @Override
                    public T next() {
                        return withContainer(serializer.<T>bytesToObject(resultIterator.next()));
                    }
                };
            }
        };
    }

    @Override
    public boolean containsKey(Object key) {
        try (Handle handle = dbi.open()){
            return handle
                    .createQuery(String.format(
                            "select count(*) %s from %s where %s = ?", keyColumnName, tableName, keyColumnName))
                    .bind(0, key)
                    .map(IntegerMapper.FIRST)
                    .first() > 0;
        }
    }

    @Override
    public boolean containsValue(Object value) {
        try (Handle handle = dbi.open()) {
            return handle
                    .createQuery(String.format(
                            "select count(*) %s from %s where %s = ?", valueColumnName, tableName, valueColumnName))
                    .bind(0, serializer.objectToBytes(value))
                    .map(IntegerMapper.FIRST)
                    .first() > 0;
        }
    }

    @Override
    public int size() {
        try (Handle handle = dbi.open()) {
            return handle
                    .createQuery(String.format("select count(*) from %s", tableName))
                    .map(IntegerMapper.FIRST)
                    .first();
        }
    }

    @Override
    public void clear() {
        try (Handle handle = dbi.open()) {
            handle.execute(String.format("delete from %s", tableName));
        }
    }

    @Override
    public T remove(Object key) {
        T value = get(key);
        try (Handle handle = dbi.open()) {
            handle.execute(String.format("delete from %s where %s = ?", tableName, keyColumnName), key);
            return withoutContainer(value);
        }
    }

    @Override
    public T get(Object key) {
        try (Handle handle = dbi.open()) {
            return withContainer(serializer.<T>bytesToObject(handle
                    .createQuery(String.format(
                            "select %s from %s where %s = ?", valueColumnName, tableName, keyColumnName))
                    .bind(0, key)
                    .map(ByteArrayMapper.FIRST)
                    .first()));
        }
    }

    @Override
    public T put(String key, T value) {
        byte[] byteArrayValue = serializer.objectToBytes(withContainer(value));
        T previous = get(key);
        try (Handle handle = dbi.open()) {
            if (previous == null) {
                handle.execute(String.format(
                        "insert into %s (%s, %s) values (?, ?)", tableName, keyColumnName, valueColumnName),
                        key, byteArrayValue);
            } else {
                handle.execute(String.format(
                        "update %s set %s = ? where %s = ?", tableName, valueColumnName, keyColumnName),
                        byteArrayValue, key);
            }
            updateAdditionalColumns(handle, key, value);
        }

        return withoutContainer(previous);
    }

    private void updateAdditionalColumns(Handle handle, String key, T value) {
        Map<String, Object> additional = additionalColumns(key, value);
        if (!additional.isEmpty()) {
            StringBuilder updateSql = new StringBuilder(String.format("update %s set ", tableName));
            List<Object> positionalParams = new ArrayList<>();
            boolean first = true;
            for (Entry column : additional.entrySet()) {
                if (first) {
                    updateSql.append(String.format("%s = ?", column.getKey()));
                    first = false;
                } else {
                    updateSql.append(String.format(", %s = ?", column.getKey()));
                }
                positionalParams.add(column.getValue());
            }
            updateSql.append(String.format(" where %s = ?", keyColumnName));
            positionalParams.add(key);
            handle.execute(updateSql.toString(), positionalParams.toArray());
        }
    }

    public Iterator<T> query(String where, Object... positionalParams) {
        final Handle handle = dbi.open();
        Query<Map<String, Object>> query1 = handle.createQuery(String.format(
                "select %s from %s where %s order by %s", valueColumnName, tableName, where, keyColumnName));
        int i = 0;
        for (Object param : positionalParams) {
            query1 = query1.bind(i++, param);
        }
        final Query<byte[]> query2 = query1.map(ByteArrayMapper.FIRST);

        return new QueryResultIterator<T, byte[]>(query2, handle) {
            @Override
            public T next() {
                return withContainer(serializer.<T>bytesToObject(resultIterator.next()));
            }
        };
    }

    public Iterator<T> query(String where, Map<String, Object> namedParams) {
        final Handle handle = dbi.open();
        Query<Map<String, Object>> query1 = handle.createQuery(String.format(
                "select %s from %s where %s order by %s", valueColumnName, tableName, where, keyColumnName));
        for (Map.Entry<String, Object> param : namedParams.entrySet()) {
            query1 = query1.bind(param.getKey(), param.getValue());
        }
        final Query<byte[]> query2 = query1.map(ByteArrayMapper.FIRST);

        return new QueryResultIterator<T, byte[]>(query2, handle) {
            @Override
            public T next() {
                return withContainer(serializer.<T>bytesToObject(resultIterator.next()));
            }
        };
    }

    @SuppressWarnings("unused")
    protected Map<String, Object> additionalColumns(String key, T value) {
        // subclasses can override to supply additional column data to be stored, for supporting custom queries.
        return Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    private T withContainer(T value) {
        if (value instanceof Storable) {
            ((Storable<T, Object>) value).setContainer(this, storableContext);
        }
        return value;
    }

    private T withoutContainer(T value) {
        if (value instanceof Storable) {
            ((Storable<?, ?>) value).setContainer(null, null);
        }
        return value;
    }

    private abstract class IteratingSet<V> extends AbstractSet<V> implements Closeable {
        private Iterator<V> iterator;

        @Override
        public final Iterator<V> iterator() {
            if (iterator != null) throw new IllegalStateException("can't ask for iterator more than once");
            iterator = createIterator();
            return iterator;
        }

        @Override
        public int size() {
            return SqlMap.this.size();
        }

        @Override
        public void clear() {
            SqlMap.this.clear();
        }

        @Override
        public boolean contains(Object v) {
            return SqlMap.this.containsValue(v);
        }

        protected abstract Iterator<V> createIterator();

        @Override
        public void close() {
            CloseableUtils.closeQuietly(iterator);
        }
    }

    private class QueryResultIterator<E, V> implements Iterator<E>, Closeable {
        protected final Handle handle;
        protected final ResultIterator<V> resultIterator;
        private final Throwable creatorTrace;
        private boolean closed = false;

        QueryResultIterator(Query<V> query, Handle handle) {
            this.creatorTrace = new Throwable();
            this.handle = handle;
            this.resultIterator = query.iterator();
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = resultIterator.hasNext();
            if (!hasNext) {
                close();
            }
            return hasNext;
        }

        @SuppressWarnings("unchecked")
        @Override
        public E next() {
            return (E) resultIterator.next();
        }

        @Override
        public void remove() {
            resultIterator.remove();
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            // If the iterator isn't completely consumed, then this provides a fallback to close the
            // handle, which holds an open JDBC connection.
            if (!closed) {
                LOGGER.warn("closing QueryResultIterator handle from finalizer", creatorTrace);
                close();
            }
        }

        public void close() {
            handle.close();
            closed = true;
        }
    }
}
