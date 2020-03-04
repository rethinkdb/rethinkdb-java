package com.rethinkdb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.exc.ReqlQueryLogicError;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Result;
import net.jodah.concurrentunit.Waiter;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class RethinkDBTest {

    public static final RethinkDB r = RethinkDB.r;
    Connection conn;
    public static final String dbName = "javatests";
    public static final String tableName = "atest";

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

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
    public void testBooleans() throws Exception {
        Boolean t = r.expr(true).run(conn, Boolean.class).single();
        assertEquals(true, t);

        Boolean f = r.expr(false).run(conn, Boolean.class).single();
        assertEquals(false, f);

        String trueType = r.expr(true).typeOf().run(conn, String.class).single();
        assertEquals("BOOL", trueType);

        String falseString = r.expr(false).coerceTo("string").run(conn, String.class).single();
        assertEquals("false", falseString);

        Boolean boolCoerce = r.expr(true).coerceTo("bool").run(conn, Boolean.class).single();
        assertEquals(true, boolCoerce);
    }

    @Test
    public void testNull() {
        Object o = r.expr(null).run(conn).single();
        assertNull(o);

        String nullType = r.expr(null).typeOf().run(conn, String.class).single();
        assertEquals("NULL", nullType);

        String nullCoerce = r.expr(null).coerceTo("string").run(conn, String.class).single();
        assertEquals("null", nullCoerce);

        Object n = r.expr(null).coerceTo("null").run(conn).single();
        assertNull(n);
    }

    @Test
    public void testString() {
        String str = r.expr("str").run(conn, String.class).single();
        assertEquals("str", str);

        String unicode = r.expr("こんにちは").run(conn, String.class).single();
        assertEquals("こんにちは", unicode);

        String strType = r.expr("foo").typeOf().run(conn, String.class).single();
        assertEquals("STRING", strType);

        String strCoerce = r.expr("foo").coerceTo("string").run(conn, String.class).single();
        assertEquals("foo", strCoerce);

        Number nmb12 = r.expr("-1.2").coerceTo("NUMBER").run(conn, Number.class).single();
        assertEquals(-1.2, nmb12);

        Long nmb10 = r.expr("0xa").coerceTo("NUMBER").run(conn, Long.class).single();
        assertNotNull(nmb10);
        assertEquals(10L, nmb10.longValue());
    }

    @Test
    public void testDate() {
        OffsetDateTime date = OffsetDateTime.now();
        OffsetDateTime result = r.expr(date).run(conn, OffsetDateTime.class).single();
        assertEquals(date, result);
    }

    @Test
    public void testCoerceFailureDoubleNegative() {
        expectedEx.expect(ReqlQueryLogicError.class);
        expectedEx.expectMessage("Could not coerce `--1.2` to NUMBER.");
        r.expr("--1.2").coerceTo("NUMBER").run(conn).single();
    }

    @Test
    public void testCoerceFailureTrailingNegative() {
        expectedEx.expect(ReqlQueryLogicError.class);
        expectedEx.expectMessage("Could not coerce `-1.2-` to NUMBER.");
        r.expr("-1.2-").coerceTo("NUMBER").run(conn).single();
    }

    @Test
    public void testCoerceFailureInfinity() {
        expectedEx.expect(ReqlQueryLogicError.class);
        expectedEx.expectMessage("Non-finite number: inf");
        r.expr("inf").coerceTo("NUMBER").run(conn).single();
    }

    @Test
    public void testSplitEdgeCases() {
        TypeReference<List<String>> typeRef = new TypeReference<List<String>>() {};
        List<String> emptySplitNothing = r.expr("").split().run(conn, typeRef).single();
        assertEquals(emptyList(), emptySplitNothing);

        List<String> nullSplit = r.expr("").split(null).run(conn, typeRef).single();
        assertEquals(emptyList(), nullSplit);

        List<String> emptySplitSpace = r.expr("").split(" ").run(conn, typeRef).single();
        assertEquals(singletonList(""), emptySplitSpace);

        List<String> emptySplitEmpty = r.expr("").split("").run(conn, typeRef).single();
        assertEquals(emptyList(), emptySplitEmpty);

        List<String> emptySplitNull5 = r.expr("").split(null, 5).run(conn, typeRef).single();
        assertEquals(emptyList(), emptySplitNull5);

        List<String> emptySplitSpace5 = r.expr("").split(" ", 5).run(conn, typeRef).single();
        assertEquals(singletonList(""), emptySplitSpace5);

        List<String> emptySplitEmpty5 = r.expr("").split("", 5).run(conn, typeRef).single();
        assertEquals(emptyList(), emptySplitEmpty5);
    }

    @Test
    public void testSplitWithNullOrWhitespace() {
        TypeReference<List<String>> typeRef = new TypeReference<List<String>>() {};
        List<String> extraWhitespace = r.expr("aaaa bbbb  cccc ").split().run(conn, typeRef).single();
        assertEquals(Arrays.asList("aaaa", "bbbb", "cccc"), extraWhitespace);

        List<String> extraWhitespaceNull = r.expr("aaaa bbbb  cccc ").split(null).run(conn, typeRef).single();
        assertEquals(Arrays.asList("aaaa", "bbbb", "cccc"), extraWhitespaceNull);

        List<String> extraWhitespaceSpace = r.expr("aaaa bbbb  cccc ").split(" ").run(conn, typeRef).single();
        assertEquals(Arrays.asList("aaaa", "bbbb", "", "cccc", ""), extraWhitespaceSpace);

        List<String> extraWhitespaceEmpty = r.expr("aaaa bbbb  cccc ").split("").run(conn, typeRef).single();
        assertEquals(Arrays.asList("a", "a", "a", "a", " ",
            "b", "b", "b", "b", " ", " ", "c", "c", "c", "c", " "), extraWhitespaceEmpty);
    }

    @Test
    public void testSplitWithString() {
        TypeReference<List<String>> typeRef = new TypeReference<List<String>>() {};
        List<String> b = r.expr("aaaa bbbb  cccc ").split("b").run(conn, typeRef).single();
        assertEquals(Arrays.asList("aaaa ", "", "", "", "  cccc "), b);
    }

    @Test
    public void testTableInsert() {
        TypeReference<MapObject<String, Object>> typeRef = new TypeReference<MapObject<String, Object>>() {};
        MapObject<Object, Object> foo = r.hashMap("hi", "There")
            .with("yes", 7)
            .with("no", null);
        Map<String, Object> result = r.db(dbName).table(tableName).insert(foo).run(conn, typeRef).single();
        assertNotNull(result);
        assertEquals(1L, result.get("inserted"));
    }

    @Test
    public void testDbGlobalArgInserted() {
        TypeReference<MapObject<String, Object>> typeRef = new TypeReference<MapObject<String, Object>>() {};
        final String tblName = "test_global_optargs";

        try {
            r.dbCreate("test").run(conn);
        } catch (Exception ignored) {
        }

        r.expr(r.array("optargs", "conn_default")).forEach(r::dbCreate).run(conn);
        r.expr(r.array("test", "optargs", "conn_default")).forEach(dbName ->
            r.db(dbName).tableCreate(tblName).do_((unused) ->
                r.db(dbName).table(tblName).insert(r.hashMap("dbName", dbName).with("id", 1))
            )
        ).run(conn);

        try {
            // no optarg set, no default db
            conn.use(null);
            Map<String, Object> x1 = r.table(tblName).get(1).run(conn, typeRef).single();
            assertNotNull(x1);
            assertEquals("test", x1.get("dbName"));

            // no optarg set, default db set
            conn.use("conn_default");
            Map<String, Object> x2 = r.table(tblName).get(1).run(conn, typeRef).single();
            assertNotNull(x2);
            assertEquals("conn_default", x2.get("dbName"));

            // optarg set, no default db
            conn.use(null);
            Map<String, Object> x3 = r.table(tblName).get(1).run(conn, OptArgs.of("db", "optargs"), typeRef).single();
            assertNotNull(x3);
            assertEquals("optargs", x3.get("dbName"));

            // optarg set, default db
            conn.use("conn_default");
            Map<String, Object> x4 = r.table(tblName).get(1).run(conn, OptArgs.of("db", "optargs"), typeRef).single();
            assertNotNull(x4);
            assertEquals("optargs", x4.get("dbName"));

        } finally {
            conn.use(null);
            r.expr(r.array("optargs", "conn_default")).forEach(r::dbDrop).run(conn);
            r.db("test").tableDrop(tblName).run(conn);
        }
    }

    @Test
    public void testFilter() {
        r.db(dbName).table(tableName).insert(r.hashMap("field", "123")).run(conn);
        r.db(dbName).table(tableName).insert(r.hashMap("field", "456")).run(conn);

        assertEquals(2, r.db(dbName).table(tableName).run(conn).toList().size());

        // The following won't work, because r.row is not implemented in the Java driver. Use lambda syntax instead
        // Cursor<Map<String, String>> oneEntryRow = r.db(dbName).table(tableName).filter(r.row("field").eq("456")).run(conn);
        // assertEquals(1, oneEntryRow.toList().size());

        assertEquals(1, r.db(dbName).table(tableName).filter(table -> table.getField("field").eq("456")).run(conn).toList().size());
    }

    @Test
    public void testResultTryWithResources() {
        r.db(dbName).table(tableName).insert(r.hashMap("field", "123")).run(conn);
        r.db(dbName).table(tableName).insert(r.hashMap("field", "456")).run(conn);

        try (Result<Object> allEntries = r.db(dbName).table(tableName).run(conn)) {
            assertEquals(2, allEntries.toList().size());
        }
    }

    @Test
    public void testTableSelectOfPojo() {
        TypeReference<MapObject<String, Object>> typeRef = new TypeReference<MapObject<String, Object>>() {};

        TestPojo pojo = new TestPojo("foo", new TestPojoInner(42L, true));
        Map<String, Object> pojoResult = r.db(dbName).table(tableName).insert(pojo).run(conn, typeRef).single();
        assertNotNull(pojoResult);
        assertEquals(1L, pojoResult.get("inserted"));

        String key = (String) ((List) pojoResult.get("generated_keys")).get(0);
        TestPojo result = r.db(dbName).table(tableName).get(key).run(conn, TestPojo.class).single();

        assertNotNull(result);
        assertEquals("foo", result.getStringProperty());
        assertEquals(42L, result.getPojoProperty().getLongProperty().longValue());
        assertEquals(true, result.getPojoProperty().getBooleanProperty());
    }

    @Test
    public void testTableSelectOfPojoSequence() {
        TypeReference<MapObject<String, Object>> typeRef = new TypeReference<MapObject<String, Object>>() {};

        TestPojo pojoOne = new TestPojo("foo", new TestPojoInner(42L, true));
        TestPojo pojoTwo = new TestPojo("bar", new TestPojoInner(53L, false));
        Map<String, Object> pojoOneResult = r.db(dbName).table(tableName).insert(pojoOne).run(conn, typeRef).single();
        Map<String, Object> pojoTwoResult = r.db(dbName).table(tableName).insert(pojoTwo).run(conn, typeRef).single();
        assertNotNull(pojoOneResult);
        assertNotNull(pojoTwoResult);
        assertEquals(1L, pojoOneResult.get("inserted"));
        assertEquals(1L, pojoTwoResult.get("inserted"));

        List<TestPojo> result = r.db(dbName).table(tableName).run(conn, TestPojo.class).toList();
        assertEquals(2L, result.size());

        TestPojo pojoOneSelected = "foo".equals(result.get(0).getStringProperty()) ? result.get(0) : result.get(1);
        TestPojo pojoTwoSelected = "bar".equals(result.get(0).getStringProperty()) ? result.get(0) : result.get(1);

        assertEquals("foo", pojoOneSelected.getStringProperty());
        assertEquals(42L, (long) pojoOneSelected.getPojoProperty().getLongProperty());
        assertEquals(true, pojoOneSelected.getPojoProperty().getBooleanProperty());

        assertEquals("bar", pojoTwoSelected.getStringProperty());
        assertEquals(53L, (long) pojoTwoSelected.getPojoProperty().getLongProperty());
        assertEquals(false, pojoTwoSelected.getPojoProperty().getBooleanProperty());
    }

    @Test(timeout = 40000)
    public void testConcurrentWrites() throws TimeoutException, InterruptedException {
        TypeReference<MapObject<String, Object>> typeRef = new TypeReference<MapObject<String, Object>>() {};
        final int total = 500;
        final AtomicInteger writeCounter = new AtomicInteger(0);
        final Waiter waiter = new Waiter();
        for (int i = 0; i < total; i++)
            new Thread(() -> {
                final TestPojo pojo = new TestPojo("writezz", new TestPojoInner(10L, true));
                final Map<String, Object> result = r.db(dbName).table(tableName).insert(pojo).run(conn, typeRef).single();
                waiter.assertNotNull(result);
                waiter.assertEquals(1L, Objects.requireNonNull(result).get("inserted"));
                writeCounter.getAndIncrement();
                waiter.resume();
            }).start();

        waiter.await(5000, total);

        assertEquals(total, writeCounter.get());
    }

    @Test(timeout = 20000)
    public void testConcurrentReads() throws TimeoutException, InterruptedException {
        TypeReference<MapObject<String, Object>> typeRef = new TypeReference<MapObject<String, Object>>() {};
        final int total = 500;
        final AtomicInteger readCounter = new AtomicInteger(0);

        // write to the database and retrieve the id
        final TestPojo pojo = new TestPojo("readzz", new TestPojoInner(10L, true));
        final Map<String, Object> result = r.db(dbName).table(tableName).insert(pojo)
            .optArg("return_changes", true).run(conn, typeRef).single();
        assertNotNull(result);
        final String id = ((List<?>) result.get("generated_keys")).get(0).toString();

        final Waiter waiter = new Waiter();
        for (int i = 0; i < total; i++)
            new Thread(() -> {
                // make sure there's only one
                final Result<TestPojo> sequence = r.db(dbName).table(tableName).run(conn, TestPojo.class);
                assertEquals(1, sequence.toList().size());
                // read that one
                final TestPojo readPojo = r.db(dbName).table(tableName).get(id).run(conn, TestPojo.class).first();
                waiter.assertNotNull(readPojo);
                // assert inserted values
                waiter.assertEquals("readzz", Objects.requireNonNull(readPojo).getStringProperty());
                waiter.assertEquals(10L, readPojo.getPojoProperty().getLongProperty());
                waiter.assertEquals(true, readPojo.getPojoProperty().getBooleanProperty());
                readCounter.getAndIncrement();
                waiter.resume();
            }).start();

        waiter.await(10000, total);

        assertEquals(total, readCounter.get());
    }

    @Test(timeout = 20000)
    public void testConcurrentCursor() throws TimeoutException, InterruptedException {
        TypeReference<MapObject<String, Object>> typeRef = new TypeReference<MapObject<String, Object>>() {};
        final int total = 500;
        final Waiter waiter = new Waiter();
        for (int i = 0; i < total; i++)
            new Thread(() -> {
                final TestPojo pojo = new TestPojo("writezz", new TestPojoInner(10L, true));
                final Map<String, Object> result = r.db(dbName).table(tableName).insert(pojo).run(conn, typeRef).single();
                waiter.assertNotNull(result);
                waiter.assertEquals(1L, Objects.requireNonNull(result).get("inserted"));
                waiter.resume();
            }).start();

        waiter.await(2500, total);

        final Result<TestPojo> all = r.db(dbName).table(tableName).run(conn, TestPojo.class);
        assertEquals(total, all.toList().size());
    }

    @Test
    public void testNoreply() throws Exception {
        r.expr(null).runNoReply(conn);
    }
}

