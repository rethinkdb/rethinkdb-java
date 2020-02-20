package com.rethinkdb;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rethinkdb.gen.model.TopLevel;
import com.rethinkdb.net.Connection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;

/**
 * Your entry point to interacting with RethinkDB.
 */
public class RethinkDB extends TopLevel {
    /**
     * The Singleton to use to begin interacting with RethinkDB Driver
     */
    public static final RethinkDB r = new RethinkDB();

    /**
     * Jackson's {@link ObjectMapper} for internal JSON handling from and to RethinkDB's internals.
     */
    private static ObjectMapper internalMapper;
    /**
     * Jackson's {@link ObjectMapper} for handling {@link com.rethinkdb.net.Result}'s values.
     */
    private static ObjectMapper resultMapper;

    /**
     * Gets (or creates, if null) the {@link ObjectMapper} for internal JSON handling from and to RethinkDB's internals.
     * <br><br>
     * <b>WARNING:If you're trying to get or configure the {@link com.rethinkdb.net.Result}'s mapper,
     * use {@link RethinkDB#getResultMapper()} instead.</b>
     *
     * @return the internal {@link ObjectMapper}
     */
    public synchronized static @NotNull ObjectMapper getInternalMapper() {
        ObjectMapper mapper = internalMapper;
        if (mapper == null) {
            mapper = new ObjectMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.USE_LONG_FOR_INTS);
            internalMapper = mapper;
        }
        return mapper;
    }

    /**
     * Sets the {@link ObjectMapper} for internal JSON handling from and to RethinkDB's internals.
     * <br><br>
     * <b>WARNING:If you're trying to set the {@link com.rethinkdb.net.Result}'s mapper,
     * use {@link RethinkDB#setResultMapper(ObjectMapper)} instead.</b>
     *
     * @param mapper an {@link ObjectMapper}, or null
     */
    public synchronized static void setInternalMapper(@Nullable ObjectMapper mapper) {
        internalMapper = mapper;
    }

    /**
     * Gets (or creates, if null) the {@link ObjectMapper} for handling {@link com.rethinkdb.net.Result}'s values.
     *
     * @return the {@link com.rethinkdb.net.Result}'s {@link ObjectMapper}
     */
    public synchronized static @NotNull ObjectMapper getResultMapper() {
        ObjectMapper mapper = resultMapper;
        if (mapper == null) {
            mapper = new ObjectMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            resultMapper = mapper;
        }
        return mapper;
    }

    /**
     * Sets the {@link ObjectMapper} for handling {@link com.rethinkdb.net.Result}'s values.
     *
     * @param mapper an {@link ObjectMapper}, or null
     */
    public synchronized static void setResultMapper(@Nullable ObjectMapper mapper) {
        resultMapper = mapper;
    }

    /**
     * Creates a new connection builder.
     *
     * @return a newly created {@link Connection.Builder}
     */
    public @NotNull Connection.Builder connection() {
        return new Connection.Builder();
    }

    /**
     * Creates a new connection builder and configures it with a db-url.
     *
     * @param dburl the db-url to configure the builder.
     * @return a newly created {@link Connection.Builder}
     */
    public @NotNull Connection.Builder connection(@NotNull String dburl) {
        return connection(URI.create(dburl));
    }

    /**
     * Creates a new connection builder and configures it with a db-url.
     *
     * @param uri the db-url to configure the builder.
     * @return a newly created {@link Connection.Builder}
     */
    public @NotNull Connection.Builder connection(@NotNull URI uri) {
        return new Connection.Builder(uri);
    }
}
