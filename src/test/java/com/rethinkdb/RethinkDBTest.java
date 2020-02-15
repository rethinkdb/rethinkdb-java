package com.rethinkdb;

import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.exc.ReqlQueryLogicError;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Connection;
import net.jodah.concurrentunit.Waiter;
import org.junit.*;
import org.junit.rules.ExpectedException;
import reactor.core.publisher.Flux;

import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RethinkDBTest{

    public static final RethinkDB r = RethinkDB.r;
    Connection conn;
    public static final String dbName = "javatests";
    public static final String tableName = "atest";

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        Connection conn = TestingFramework.createConnection();
        try{
            r.dbCreate(dbName).run(conn);
        } catch(ReqlError e){}
        try {
            r.db(dbName).wait_().run(conn);
            r.db(dbName).tableCreate(tableName).run(conn);
            r.db(dbName).table(tableName).wait_().run(conn);
        } catch(ReqlError e){}
        conn.close();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        Connection conn = TestingFramework.createConnection();
        try {
            r.db(dbName).tableDrop(tableName).run(conn);
            r.dbDrop(dbName).run(conn);
        } catch(ReqlError e){}
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
        Boolean t = r.expr(true).run(conn).cast(Boolean.class).blockFirst();
        Assert.assertEquals(true, t.booleanValue());

        Boolean f = r.expr(false).run(conn).cast(Boolean.class).blockFirst();
        Assert.assertEquals(false, f.booleanValue());

        String trueType = r.expr(true).typeOf().run(conn).cast(String.class).blockFirst();
        Assert.assertEquals("BOOL", trueType);

        String falseString = r.expr(false).coerceTo("string").run(conn).cast(String.class).blockFirst();
        Assert.assertEquals("false", falseString);

        Boolean boolCoerce = r.expr(true).coerceTo("bool").run(conn).cast(Boolean.class).blockFirst();
        Assert.assertEquals(true, boolCoerce.booleanValue());
    }

    @Test
    public void testNull() {
        Object o = r.expr(null).run(conn).blockFirst();
        Assert.assertEquals(null, o);

        String nullType = r.expr(null).typeOf().run(conn).cast(String.class).blockFirst();
        Assert.assertEquals("NULL", nullType);

        String nullCoerce = r.expr(null).coerceTo("string").run(conn).cast(String.class).blockFirst();
        Assert.assertEquals("null", nullCoerce);

        Object n = r.expr(null).coerceTo("null").run(conn).blockFirst();
        Assert.assertEquals(null, n);
    }

    @Test
    public void testString() {
        String str = r.expr("str").run(conn).cast(String.class).blockFirst();
        Assert.assertEquals("str", str);

        String unicode = r.expr("こんにちは").run(conn).cast(String.class).blockFirst();
        Assert.assertEquals("こんにちは", unicode);

        String strType = r.expr("foo").typeOf().run(conn).cast(String.class).blockFirst();
        Assert.assertEquals("STRING", strType);

        String strCoerce = r.expr("foo").coerceTo("string").run(conn).cast(String.class).blockFirst();
        Assert.assertEquals("foo", strCoerce);

        Number nmb12 = r.expr("-1.2").coerceTo("NUMBER").run(conn).cast(Number.class).blockFirst();
        Assert.assertEquals(-1.2, nmb12);

        Long nmb10 = r.expr("0xa").coerceTo("NUMBER").run(conn).cast(Long.class).blockFirst();
        Assert.assertEquals(10L, nmb10.longValue());
    }

    @Test
    public void testDate() {
        OffsetDateTime date = OffsetDateTime.now();
        OffsetDateTime result = r.expr(date).run(conn).cast(OffsetDateTime.class).blockFirst();
        Assert.assertEquals(date, result);
    }

    @Test
    public void testCoerceFailureDoubleNegative() {
        expectedEx.expect(ReqlQueryLogicError.class);
        expectedEx.expectMessage("Could not coerce `--1.2` to NUMBER.");
        r.expr("--1.2").coerceTo("NUMBER").run(conn);
    }

    @Test
    public void testCoerceFailureTrailingNegative() {
        expectedEx.expect(ReqlQueryLogicError.class);
        expectedEx.expectMessage("Could not coerce `-1.2-` to NUMBER.");
        r.expr("-1.2-").coerceTo("NUMBER").run(conn);
    }

    @Test
    public void testCoerceFailureInfinity() {
        expectedEx.expect(ReqlQueryLogicError.class);
        expectedEx.expectMessage("Non-finite number: inf");
        r.expr("inf").coerceTo("NUMBER").run(conn);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSplitEdgeCases() {
        List<String> emptySplitNothing = r.expr("").split().run(conn).cast(List.class).blockFirst();
        Assert.assertEquals(emptySplitNothing, Arrays.asList());

        List<String> nullSplit = r.expr("").split(null).run(conn).cast(List.class).blockFirst();
        Assert.assertEquals(nullSplit, Arrays.asList());

        List<String> emptySplitSpace = r.expr("").split(" ").run(conn).cast(List.class).blockFirst();
        Assert.assertEquals(Arrays.asList(""), emptySplitSpace);

        List<String> emptySplitEmpty = r.expr("").split("").run(conn).cast(List.class).blockFirst();
        assertEquals(Arrays.asList(), emptySplitEmpty);

        List<String> emptySplitNull5 = r.expr("").split(null, 5).run(conn).cast(List.class).blockFirst();
        assertEquals(Arrays.asList(), emptySplitNull5);

        List<String> emptySplitSpace5 = r.expr("").split(" ", 5).run(conn).cast(List.class).blockFirst();
        assertEquals(Arrays.asList(""), emptySplitSpace5);

        List<String> emptySplitEmpty5 = r.expr("").split("", 5).run(conn).cast(List.class).blockFirst();
        assertEquals(Arrays.asList(), emptySplitEmpty5);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSplitWithNullOrWhitespace() {
        List<String> extraWhitespace = r.expr("aaaa bbbb  cccc ").split().run(conn).cast(List.class).blockFirst();
        assertEquals(Arrays.asList("aaaa", "bbbb", "cccc"), extraWhitespace);

        List<String> extraWhitespaceNull = r.expr("aaaa bbbb  cccc ").split(null).run(conn).cast(List.class).blockFirst();
        assertEquals(Arrays.asList("aaaa", "bbbb", "cccc"), extraWhitespaceNull);

        List<String> extraWhitespaceSpace = r.expr("aaaa bbbb  cccc ").split(" ").run(conn).cast(List.class).blockFirst();
        assertEquals(Arrays.asList("aaaa", "bbbb", "", "cccc", ""), extraWhitespaceSpace);

        List<String> extraWhitespaceEmpty = r.expr("aaaa bbbb  cccc ").split("").run(conn).cast(List.class).blockFirst();
        assertEquals(Arrays.asList("a", "a", "a", "a", " ",
                "b", "b", "b", "b", " ", " ", "c", "c", "c", "c", " "), extraWhitespaceEmpty);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSplitWithString() {
        List<String> b = r.expr("aaaa bbbb  cccc ").split("b").run(conn).cast(List.class).blockFirst();
        assertEquals(Arrays.asList("aaaa ", "", "", "", "  cccc "), b);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testTableInsert(){
        MapObject foo = new MapObject()
                .with("hi", "There")
                .with("yes", 7)
                .with("no", null );
        Map<String, Object> result = r.db(dbName).table(tableName).insert(foo).run(conn).cast(Map.class).blockFirst();
        assertEquals(1L, result.get("inserted"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDbGlobalArgInserted() {
        final String tblName = "test_global_optargs";

        try {
            r.dbCreate("test").run(conn);
        } catch (Exception e) {}

        r.expr(r.array("optargs", "conn_default")).forEach(r::dbCreate).run(conn);
        r.expr(r.array("test", "optargs", "conn_default")).forEach(dbName ->
                        r.db(dbName).tableCreate(tblName).do_((unused) ->
                                        r.db(dbName).table(tblName).insert(r.hashMap("dbName", dbName).with("id", 1))
                        )
        ).run(conn);

        try {
            // no optarg set, no default db
            conn.use(null);
            Map x1 = r.table(tblName).get(1).run(conn).cast(Map.class).blockFirst();
            assertEquals("test", x1.get("dbName"));

            // no optarg set, default db set
            conn.use("conn_default");
            Map x2 = r.table(tblName).get(1).run(conn).cast(Map.class).blockFirst();
            assertEquals("conn_default", x2.get("dbName"));

            // optarg set, no default db
            conn.use(null);
            Map x3 = r.table(tblName).get(1).run(conn, OptArgs.of("db", "optargs")).cast(Map.class).blockFirst();
            assertEquals("optargs", x3.get("dbName"));

            // optarg set, default db
            conn.use("conn_default");
            Map x4 = r.table(tblName).get(1).run(conn, OptArgs.of("db", "optargs")).cast(Map.class).blockFirst();
            assertEquals("optargs", x4.get("dbName"));

        } finally {
            conn.use(null);
            r.expr(r.array("optargs", "conn_default")).forEach(r::dbDrop).run(conn);
            r.db("test").tableDrop(tblName).run(conn);
        }
    }

    @Test
    public void testFilter() {
        r.db(dbName).table(tableName).insert(new MapObject().with("field", "123")).run(conn);
        r.db(dbName).table(tableName).insert(new MapObject().with("field", "456")).run(conn);

        Flux<Object> allEntries = r.db(dbName).table(tableName).run(conn);
        assertEquals(2, allEntries.count().block().longValue());

        // The following won't work, because r.row is not implemented in the Java driver. Use lambda syntax instead
        // Cursor<Map<String, String>> oneEntryRow = r.db(dbName).table(tableName).filter(r.row("field").eq("456")).run(conn);
        // assertEquals(1, oneEntryRow.toList().size());

        Flux<Object> oneEntryLambda = r.db(dbName).table(tableName).filter(table -> table.getField("field").eq("456")).run(conn);
        assertEquals(1, oneEntryLambda.count().block().intValue());
    }

    @Test
    public void testTableSelectOfPojo() {
        TestPojo pojo = new TestPojo("foo", new TestPojoInner(42L, true));
        Map<String, Object> pojoResult = r.db(dbName).table(tableName).insert(pojo).run(conn).cast(Map.class).blockFirst();
        assertEquals(1L, pojoResult.get("inserted"));

        String key = (String) ((List) pojoResult.get("generated_keys")).get(0);
        TestPojo result = r.db(dbName).table(tableName).get(key).run(conn, TestPojo.class).blockFirst();

        assertEquals("foo", result.getStringProperty());
        assertTrue(42L == result.getPojoProperty().getLongProperty());
        assertEquals(true, result.getPojoProperty().getBooleanProperty());
    }

    @Test
    public void testTableSelectOfPojoCursor() {
        TestPojo pojoOne = new TestPojo("foo", new TestPojoInner(42L, true));
        TestPojo pojoTwo = new TestPojo("bar", new TestPojoInner(53L, false));
        Map<String, Object> pojoOneResult = r.db(dbName).table(tableName).insert(pojoOne).run(conn).cast(Map.class).blockFirst();
        Map<String, Object> pojoTwoResult = r.db(dbName).table(tableName).insert(pojoTwo).run(conn).cast(Map.class).blockFirst();
        assertEquals(1L, pojoOneResult.get("inserted"));
        assertEquals(1L, pojoTwoResult.get("inserted"));

        Flux<TestPojo> cursor = r.db(dbName).table(tableName).run(conn, TestPojo.class);
        List<TestPojo> result = Objects.requireNonNull(cursor.collectList().block());
        assertEquals(2L, result.size());

        TestPojo pojoOneSelected = "foo".equals(result.get(0).getStringProperty()) ? result.get(0) : result.get(1);
        TestPojo pojoTwoSelected = "bar".equals(result.get(0).getStringProperty()) ? result.get(0) : result.get(1);

        assertEquals("foo", pojoOneSelected.getStringProperty());
        assertTrue(42L == pojoOneSelected.getPojoProperty().getLongProperty());
        assertEquals(true, pojoOneSelected.getPojoProperty().getBooleanProperty());

        assertEquals("bar", pojoTwoSelected.getStringProperty());
        assertTrue(53L == pojoTwoSelected.getPojoProperty().getLongProperty());
        assertEquals(false, pojoTwoSelected.getPojoProperty().getBooleanProperty());
    }

    @Test(timeout=40000)
    public void testConcurrentWrites() throws TimeoutException, InterruptedException {
        final int total = 500;
        final AtomicInteger writeCounter = new AtomicInteger(0);
        final Waiter waiter = new Waiter();
        for (int i = 0; i < total; i++)
            new Thread(() -> {
                final TestPojo pojo = new TestPojo("writezz", new TestPojoInner(10L, true));
                final Map<String, Object> result = r.db(dbName).table(tableName).insert(pojo).run(conn).cast(Map.class).blockFirst();
                waiter.assertEquals(1L, result.get("inserted"));
                writeCounter.getAndIncrement();
                waiter.resume();
            }).start();

        waiter.await(5000, total);

        assertEquals(total, writeCounter.get());
    }

    @Test(timeout=20000)
    public void testConcurrentReads() throws TimeoutException {
        final int total = 500;
        final AtomicInteger readCounter = new AtomicInteger(0);

        // write to the database and retrieve the id
        final TestPojo pojo = new TestPojo("readzz", new TestPojoInner(10L, true));
        final Map<String, Object> result = r.db(dbName).table(tableName).insert(pojo).optArg("return_changes", true).run(conn).cast(Map.class).blockFirst();
        final String id = ((List) result.get("generated_keys")).get(0).toString();

        final Waiter waiter = new Waiter();
        for (int i = 0; i < total; i++)
            new Thread(() -> {
                // make sure there's only one
                final Flux<TestPojo> cursor = r.db(dbName).table(tableName).run(conn, TestPojo.class);
                assertEquals(1, cursor.count().block().intValue());
                // read that one
                final TestPojo readPojo = r.db(dbName).table(tableName).get(id).run(conn, TestPojo.class).blockFirst();
                waiter.assertNotNull(readPojo);
                // assert inserted values
                waiter.assertEquals("readzz", readPojo.getStringProperty());
                waiter.assertEquals(10L, readPojo.getPojoProperty().getLongProperty());
                waiter.assertEquals(true, readPojo.getPojoProperty().getBooleanProperty());
                readCounter.getAndIncrement();
                waiter.resume();
            }).start();

        waiter.await(10000, total);

        assertEquals(total, readCounter.get());
    }

    @Test(timeout=20000)
    public void testConcurrentCursor() throws TimeoutException, InterruptedException {
        final int total = 500;
        final Waiter waiter = new Waiter();
        for (int i = 0; i < total; i++)
            new Thread(() -> {
                final TestPojo pojo = new TestPojo("writezz", new TestPojoInner(10L, true));
                final Map<String, Object> result = r.db(dbName).table(tableName).insert(pojo).run(conn).cast(Map.class).blockFirst();
                waiter.assertEquals(1L, result.get("inserted"));
                waiter.resume();
            }).start();

        waiter.await(2500, total);

        final Flux<TestPojo> all = r.db(dbName).table(tableName).run(conn, TestPojo.class);
        assertEquals(total, all.count().block().intValue());
    }

    @Test
    public void testNoreply() throws Exception {
        r.expr(null).runNoReply(conn);
    }

    @Test
    public void test_Changefeeds_Cursor_Close_cause_new_cursor_cause_memory_leak() throws Exception {
        Field f_cursorCache = Connection.class.getDeclaredField("tracked");
        f_cursorCache.setAccessible(true);

        Set<?> cursorCache = (Set<?>) f_cursorCache.get(conn);
        assertEquals(0, cursorCache.size());

        Flux f = r.db(dbName).table(tableName).changes().run(conn);

        assertEquals(0, cursorCache.size());
    }
}

