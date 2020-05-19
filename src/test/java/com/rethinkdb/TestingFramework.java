package com.rethinkdb;

import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Result;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static com.rethinkdb.RethinkDB.*;
import static java.nio.charset.StandardCharsets.*;

/**
 * Very basic testing framework lying miserably in the java's default package.
 */
public class TestingFramework {
    private static final String OVERRIDE_FILE_NAME = "test-dburl-override.txt";

    private static Connection.Builder builder;

    /**
     * Provision a connection builder based on the test configuration.
     * <p>
     * Put a propertiy file called "test-dburl-override.txt" in the working directory of the tests to override default values.
     * The file contents must be a RethinkDB db-url.
     *
     * @return Default connection builder.
     */
    public static Connection.Builder defaultConnectionBuilder() {
        if (builder == null) {
            File defaultFile = new File(OVERRIDE_FILE_NAME);
            if (defaultFile.exists()) {
                try {
                    builder = r.connection(new String(Files.readAllBytes(defaultFile.toPath()), UTF_8));
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            } else {
                builder = r.connection();
            }
        }
        return builder;
    }

    /**
     * @return A new connection from the configuration.
     */
    public static Connection createConnection() {
        return createConnection(defaultConnectionBuilder());
    }

    /**
     * @return A new connection from a specific builder to be used in tests where a specific connection is needed,
     * i.e. connection secured with SSL.
     */
    public static Connection createConnection(Connection.Builder builder) {
        return builder.connect();
    }
}
