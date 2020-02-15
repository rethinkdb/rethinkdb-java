package com.rethinkdb;

import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.net.Connection;
import org.junit.*;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AuthTest {
    public static final RethinkDB r = RethinkDB.r;
    static final String bogusUsername = "bogus_guy";
    static final String bogusPassword = "bogus_man+=,";

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        Connection adminConn = TestingFramework.createConnection();
        r.db("rethinkdb").table("users").insert(
                r.hashMap("id", bogusUsername)
                        .with("password", bogusPassword))
                .run(adminConn);
        adminConn.close();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        Connection adminConn = TestingFramework.createConnection();
        r.db("rethinkdb").table("users").get(bogusUsername).delete();
        adminConn.close();
    }

    @Test
    public void testConnectWithNonAdminUser() throws Exception {
        Connection bogusConn = TestingFramework.defaultConnectionBuilder().copyOf()
                .user(bogusUsername, bogusPassword).connect();
        bogusConn.close();
    }

    @Test (expected=ReqlDriverError.class)
    public void testConnectWithBothAuthKeyAndUsername() throws Exception {
        Connection bogusConn = TestingFramework.defaultConnectionBuilder().copyOf()
                .user(bogusUsername, bogusPassword).authKey("test").connect();
    }
}
