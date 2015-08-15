package org.vertexium.sql.collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.IntegerMapper;
import org.skife.jdbi.v2.util.StringMapper;
import org.vertexium.VertexiumSerializer;
import org.vertexium.serializer.xstream.XStreamVertexiumSerializer;

import javax.sql.DataSource;
import java.util.*;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class SqlMapTest {
    private static Handle handle;
    private static DataSource dataSource;
    private SqlMap<SerializableThing> sqlMap;

    private static final SerializableThing THING_1 = new SerializableThing(1);
    private static final SerializableThing THING_2 = new SerializableThing(2);
    private static final SerializableThing THING_3 = new SerializableThing(3);
    private static final SerializableThing THING_4 = new SerializableThing(4);
    private XStreamVertexiumSerializer serializer = new XStreamVertexiumSerializer();

    @BeforeClass
    public static void beforeClass() {
        dataSource = JdbcConnectionPool.create("jdbc:h2:mem:test", "username", "password");
        handle = new DBI(dataSource).open();
        createTable(handle, "map");
    }

    @Before
    public void before() {
        handle.execute("insert into map (key, value) values (?, ?)", "key1", serializer.objectToBytes(THING_1));
        handle.execute("insert into map (key, value) values (?, ?)", "key2", serializer.objectToBytes(THING_2));
        sqlMap = new SqlMap<>("map", "key", "value", dataSource, serializer);
    }

    @After
    public void after() {
        handle.execute("delete from map");
    }

    @AfterClass
    public static void afterClass() {
        dropTable(handle, "map");
        handle.close();
    }

    static void createTable(Handle handle, String tableName) {
        handle.execute(String.format(
                "create table %s (key varchar(100) primary key not null, value clob not null, num int, str varchar(100))",
                tableName));
    }

    static void dropTable(Handle handle, String tableName) {
        handle.execute(String.format("drop table %s", tableName));
    }

    @Test
    public void getReturnsStoredValue() {
        assertThat(sqlMap.get("key1"), equalTo((Object) THING_1));
        assertThat(sqlMap.get("key2"), equalTo((Object) THING_2));
    }

    @Test
    public void putStoresKeyAndValue() {
        SerializableThing previous = sqlMap.put("key3", THING_3);
        assertThat(previous, nullValue());
        assertThat(sqlMap.get("key3"), equalTo(THING_3));
        previous = sqlMap.put("key3", THING_4);
        assertThat(previous, equalTo(THING_3));
        assertThat(sqlMap.get("key3"), equalTo(THING_4));
    }

    @Test
    public void removeDeletesKeyAndValue() {
        SerializableThing previous = sqlMap.remove("key1");
        assertThat(previous, equalTo(THING_1));
        assertThat(sqlMap.get("key1"), nullValue());
        assertThat(sqlMap.size(), equalTo(1));
    }

    @Test
    public void sizeReturnsCount() {
        assertThat(sqlMap.size(), equalTo(2));
    }

    @Test
    public void clearRemovesAll() {
        sqlMap.clear();
        int count = handle.createQuery("select count (*) from map").map(IntegerMapper.FIRST).first();
        assertThat(count, equalTo(0));
    }

    @Test
    public void containsKeyReturnsTrueIfKeyIsPresent() {
        assertThat(sqlMap.containsKey("key1"), equalTo(true));
    }

    @Test
    public void containsKeyReturnsFalseIfKeyIsNotPresent() {
        assertThat(sqlMap.containsKey("nope"), equalTo(false));
    }

    @Test
    public void containsValueReturnsTrueIfValueIsPresent() {
        assertThat(sqlMap.containsValue(THING_1), equalTo(true));
    }

    @Test
    public void containsValueReturnsFalseIfValueIsNotPresent() {
        assertThat(sqlMap.containsValue(THING_3), equalTo(false));
    }

    @Test
    public void valuesReturnsAllValues() {
        Collection<SerializableThing> expected = ImmutableSet.of(THING_1, THING_2);
        assertThat(new HashSet<>(sqlMap.values()), equalTo((Object) expected));
    }

    @Test
    public void keySetReturnsAllKeys() {
        Set<String> expected = ImmutableSet.of("key1", "key2");
        assertThat(new HashSet<>(sqlMap.keySet()), equalTo(expected));
    }

    @Test
    public void entrySetReturnsAllEntries() {
        Set<Map.Entry<String, SerializableThing>> expected = ImmutableSet.<Map.Entry<String, SerializableThing>>of(
                new MapEntry<>("key1", THING_1),
                new MapEntry<>("key2", THING_2)
        );
        assertThat(new HashSet<>(sqlMap.entrySet()), equalTo(expected));
    }

    @Test
    public void putStoresExtraColumns() {
        ExtraColumnsJdbcMap extraMap = new ExtraColumnsJdbcMap("map", "key", "value", serializer);
        SerializableThing thing = new SerializableThing(42);
        extraMap.put("thing", thing);
        int num = handle.createQuery("select num from map where key = ?")
                .bind(0, "thing")
                .map(IntegerMapper.FIRST).first();
        assertThat(num, equalTo(42));
        String str = handle.createQuery("select str from map where key = ?")
                .bind(0, "thing")
                .map(StringMapper.FIRST).first();
        assertThat(str, equalTo("value42"));
        assertThat(extraMap.get("thing"), equalTo(thing));
    }

    @Test
    public void queryFindsByWhereClauseWithPositionalParams() {
        ExtraColumnsJdbcMap extraMap = new ExtraColumnsJdbcMap("map", "key", "value", serializer);
        SerializableThing thing1 = new SerializableThing(21);
        SerializableThing thing2 = new SerializableThing(42);
        SerializableThing thing3 = new SerializableThing(84);
        extraMap.put("thing1", thing1);
        extraMap.put("thing2", thing2);
        extraMap.put("thing3", thing3);
        Iterator<SerializableThing> thingIterator = extraMap.query("num > ? and str like 'value%'", 21);
        SerializableThing[] things = Iterators.toArray(thingIterator, SerializableThing.class);
        assertThat(things.length, equalTo(2));
        assertThat(things[0], equalTo(thing2));
        assertThat(things[1], equalTo(thing3));
    }

    @Test
    public void queryFindsByWhereClauseWithNamedParams() {
        ExtraColumnsJdbcMap extraMap = new ExtraColumnsJdbcMap("map", "key", "value", serializer);
        SerializableThing thing1 = new SerializableThing(21);
        SerializableThing thing2 = new SerializableThing(42);
        SerializableThing thing3 = new SerializableThing(84);
        extraMap.put("thing1", thing1);
        extraMap.put("thing2", thing2);
        extraMap.put("thing3", thing3);
        Iterator<SerializableThing> thingIterator = extraMap.query("num > :n and str like :s",
                ImmutableMap.<String, Object>of(
                        "n", 21,
                        "s", "value%"));
        SerializableThing[] things = Iterators.toArray(thingIterator, SerializableThing.class);
        assertThat(things.length, equalTo(2));
        assertThat(things[0], equalTo(thing2));
        assertThat(things[1], equalTo(thing3));
    }

    private static class ExtraColumnsJdbcMap extends SqlMap<SerializableThing> {
        public ExtraColumnsJdbcMap(String tableName, String keyColumnName, String valueColumnName, VertexiumSerializer serializer) {
            super(tableName, keyColumnName, valueColumnName, dataSource, serializer);
        }

        @Override
        protected Map<String, Object> additionalColumns(String key, SerializableThing value) {
            return ImmutableMap.<String, Object>of(
                    "num", value.i,
                    "str", value.s
            );
        }
    }
}
