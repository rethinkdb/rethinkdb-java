package com.rethinkdb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.proto.ResponseType;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Result;
import com.rethinkdb.utils.Types;
import org.junit.*;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RunHelperTest {
    private static final TypeReference<List<String>> stringList = Types.listOf(String.class);
    private static final TypeReference<Map<String, Object>> stringObjectMap = Types.mapOf(String.class, Object.class);

    public static final RethinkDB r = RethinkDB.r;
    Connection conn;
    public static final String dbName = "javatests";
    public static final String tableName = "atest";

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        Connection conn = TestingFramework.createConnection();
        try {
            r.dbCreate(dbName).run(conn);
        } catch (ReqlError e) {
        }
        try {
            r.db(dbName).wait_().run(conn);
            r.db(dbName).tableCreate(tableName).run(conn);
            r.db(dbName).table(tableName).wait_().run(conn);
        } catch (ReqlError e) {
        }
        conn.close();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        Connection conn = TestingFramework.createConnection();
        try {
            r.db(dbName).tableDrop(tableName).run(conn);
            r.dbDrop(dbName).run(conn);
        } catch (ReqlError e) {
        }
        conn.close();
    }

    @Before
    public void setUp() throws Exception {
        conn = TestingFramework.createConnection();
        r.db(dbName).table(tableName).delete().run(conn);
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }


    @Test
    public void testRun() {
        MapObject<Object, Object> expected = r.hashMap("foo", r.hashMap("num", 1L));
        Result<Object> result = r.expr(expected).run(conn);
        assertEquals(ResponseType.SUCCESS_ATOM, result.responseType());
        assertEquals(expected, result.single());
        assertFalse(conn.hasOngoingQueries());
    }

    @Test
    public void testRunAsync() {
        MapObject<Object, Object> expected = r.hashMap("foo", r.hashMap("num", 1L));
        CompletableFuture<Result<Object>> result = r.expr(expected).runAsync(conn);
        assertFalse(conn.hasOngoingQueries());
        assertEquals(ResponseType.SUCCESS_ATOM, result.thenApply(Result::responseType).join());
        assertEquals(expected, result.thenApply(Result::single).join());
    }

    @Test
    public void testRunAtom() {
        MapObject<Object, Object> expected = r.hashMap("foo", r.hashMap("num", 1L));
        assertEquals(expected, r.expr(expected).runAtom(conn));
        assertFalse(conn.hasOngoingQueries());
    }

    @Test
    public void testRunAtomAsync() {
        MapObject<Object, Object> expected = r.hashMap("foo", r.hashMap("num", 1L));
        CompletableFuture<Object> result = r.expr(expected).runAtomAsync(conn);
        assertFalse(conn.hasOngoingQueries());
        assertEquals(expected, result.join());
    }

    @Test
    public void testRunGrouping() {
        MapObject<Object, Object> obj1 = r.hashMap("name", "foo").with("value", 1L);
        MapObject<Object, Object> obj2 = r.hashMap("name", "bar").with("value", 3L);
        MapObject<Object, Object> obj3 = r.hashMap("name", "foo").with("value", 6L);
        MapObject<Object, Object> obj4 = r.hashMap("name", "bar").with("value", 11L);

        MapObject<Object, Set<Object>> expected = new MapObject<Object, Set<Object>>()
            .with("foo", new HashSet<>(r.array(obj1, obj3)))
            .with("bar", new HashSet<>(r.array(obj2, obj4)));

        Map<Object, Set<Object>> result = r.expr(r.array(obj1, obj2, obj3, obj4))
            .group("name")
            .runGrouping(conn, Object.class, Object.class);

        assertEquals(expected, result);
        assertFalse(conn.hasOngoingQueries());
    }

    @Test
    public void testRunGroupingAsync() {
        MapObject<Object, Object> obj1 = r.hashMap("name", "foo").with("value", 1L);
        MapObject<Object, Object> obj2 = r.hashMap("name", "bar").with("value", 3L);
        MapObject<Object, Object> obj3 = r.hashMap("name", "foo").with("value", 6L);
        MapObject<Object, Object> obj4 = r.hashMap("name", "bar").with("value", 11L);

        MapObject<Object, Set<Object>> expected = new MapObject<Object, Set<Object>>()
            .with("foo", new HashSet<>(r.array(obj1, obj3)))
            .with("bar", new HashSet<>(r.array(obj2, obj4)));

        CompletableFuture<Map<Object, Set<Object>>> result = r.expr(r.array(obj1, obj2, obj3, obj4))
            .group("name")
            .runGroupingAsync(conn, Object.class, Object.class);

        assertFalse(conn.hasOngoingQueries());
        assertEquals(expected, result.join());
    }

    @Test
    public void testRunUnwrapping() {
        MapObject<Object, Object> expected1 = r.hashMap("foo", r.hashMap("num", 1L));
        MapObject<Object, Object> expected2 = r.hashMap("bar", r.hashMap("num", 2L));
        MapObject<Object, Object> expected3 = r.hashMap("zzz", r.hashMap("num", 4L));
        Result<Object> result = r.expr(r.array(expected1, expected2, expected3)).runUnwrapping(conn);
        assertEquals(ResponseType.SUCCESS_ATOM, result.responseType());
        assertEquals(expected1, result.next());
        assertEquals(expected2, result.next());
        assertEquals(expected3, result.next());
        assertFalse(conn.hasOngoingQueries());
    }

    @Test
    public void testRunUnwrappingAsync() {
        MapObject<Object, Object> expected1 = r.hashMap("foo", r.hashMap("num", 1L));
        MapObject<Object, Object> expected2 = r.hashMap("bar", r.hashMap("num", 2L));
        MapObject<Object, Object> expected3 = r.hashMap("zzz", r.hashMap("num", 4L));
        CompletableFuture<Result<Object>> result = r.expr(r.array(expected1, expected2, expected3)).runUnwrappingAsync(conn);
        assertFalse(conn.hasOngoingQueries());
        assertEquals(expected1, result.thenApply(Result::next).join());
        assertEquals(expected2, result.thenApply(Result::next).join());
        assertEquals(expected3, result.thenApply(Result::next).join());
    }
}
