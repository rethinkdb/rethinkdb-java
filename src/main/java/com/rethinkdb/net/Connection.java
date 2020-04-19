package com.rethinkdb.net;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.ast.Query;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.Db;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.model.TopLevel;
import com.rethinkdb.gen.proto.ResponseType;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.model.Server;
import com.rethinkdb.net.Result.FetchMode;
import com.rethinkdb.utils.Internals;
import com.rethinkdb.utils.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * A single connection to RethinkDB.
 * <p>
 * This object is thread-safe.
 */
public class Connection implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);

    protected final ConnectionSocket.Factory socketFactory;
    protected final ResponsePump.Factory pumpFactory;
    protected final String hostname;
    protected final int port;
    protected final @Nullable SSLContext sslContext;
    protected final @Nullable Long timeout;
    protected final @Nullable String user;
    protected final @Nullable String password;
    protected final FetchMode defaultFetchMode;
    protected final boolean persistentThreads;

    protected final AtomicLong nextToken = new AtomicLong();
    protected final Set<Result<?>> tracked = ConcurrentHashMap.newKeySet();
    protected final Lock writeLock = new ReentrantLock();

    protected @Nullable String dbname;
    protected @Nullable ConnectionSocket socket;
    protected @Nullable ResponsePump pump;
    protected boolean unwrapLists;

    /**
     * Creates a new connection based on a {@link Builder}.
     *
     * @param b the connection builder
     */
    public Connection(Builder b) {
        if (b.authKey != null && b.user != null) {
            throw new ReqlDriverError("Either `authKey` or `user` can be used, but not both.");
        }
        this.socketFactory = b.socketFactory != null ? b.socketFactory : DefaultConnectionFactory.INSTANCE;
        this.pumpFactory = b.pumpFactory != null ? b.pumpFactory : DefaultConnectionFactory.INSTANCE;
        this.hostname = b.hostname != null ? b.hostname : "127.0.0.1";
        this.port = b.port != null ? b.port : 28015;
        this.dbname = b.dbname;
        this.sslContext = b.sslContext;
        this.timeout = b.timeout;
        this.user = b.user != null ? b.user : "admin";
        this.password = b.password != null ? b.password : b.authKey != null ? b.authKey : "";
        this.unwrapLists = b.unwrapLists;
        this.persistentThreads = b.persistentThreads;
        this.defaultFetchMode = b.defaultFetchMode != null ? b.defaultFetchMode : FetchMode.LAZY;
    }

    /**
     * Gets the default database of the server.
     * <br>
     * To set the default database, use {@link Builder#db(String)} or {@link Connection#use(String)}
     *
     * @return the current default database, if any, of null.
     */
    public @Nullable String db() {
        return dbname;
    }

    /**
     * Sets the default database of the server.
     *
     * @param db the new current default database.
     * @return itself.
     */
    public @NotNull Connection use(@Nullable String db) {
        dbname = db;
        return this;
    }

    /**
     * Enables or disables list unwrapping.
     *
     * @param val {@code true} to enable list unwrapping, {@code false} to disable.
     * @return itself.
     * @deprecated Use {@link ReqlAst#runUnwrapping(Connection)} and {@link ReqlAst#runAtom(Connection)} if you want to
     * always the same consistency. <b><i>(Will be removed on v2.5.0)</i></b>
     */
    @Deprecated
    public @NotNull Connection unwrapLists(boolean val) {
        unwrapLists = val;
        return this;
    }

    /**
     * Checks if the connection is open.
     *
     * @return true if the socket and the response pump are working, otherwise false.
     */
    public boolean isOpen() {
        return socket != null && socket.isOpen() && pump != null && pump.isAlive();
    }

    /**
     * Begins the socket connection to the server asynchronously.
     *
     * @return a {@link CompletableFuture} which completes with itself, once connected.
     */
    public @NotNull CompletableFuture<Connection> connectAsync() {
        if (socket != null) {
            throw new ReqlDriverError("Client already connected!");
        }
        return socketFactory.newSocketAsync(hostname, port, sslContext, timeout).thenApply(socket -> {
            this.socket = socket;
            HandshakeProtocol.doHandshake(socket, user, password, timeout);
            this.pump = pumpFactory.newPump(socket, !persistentThreads);
            return this;
        });
    }

    /**
     * Begins the socket connection to the server.
     *
     * @return itself, once connected.
     */
    public @NotNull Connection connect() {
        try {
            return connectAsync().join();
        } catch (CompletionException ce) {
            Throwable t = ce.getCause();
            if (t instanceof ReqlError) {
                throw ((ReqlError) t);
            }
            throw new ReqlDriverError(t);
        }
    }

    /**
     * Closes and reconnects to the server.
     *
     * @return a {@link CompletableFuture} which completes with itself, once reconnected.
     */
    public @NotNull CompletableFuture<Connection> reconnectAsync() {
        return reconnectAsync(true);
    }

    /**
     * Closes and reconnects to the server asynchronously.
     *
     * @param noreplyWait if closing should send a {@link Connection#noreplyWait()} before closing.
     * @return a {@link CompletableFuture} which completes with itself, once reconnected.
     */
    public @NotNull CompletableFuture<Connection> reconnectAsync(boolean noreplyWait) {
        return closeAsync(noreplyWait).thenCompose(v -> connectAsync());
    }

    /**
     * Closes and reconnects to the server.
     *
     * @return itself, once reconnected.
     */
    public @NotNull Connection reconnect() {
        return reconnect(true);
    }

    /**
     * Closes and reconnects to the server.
     *
     * @param noreplyWait if closing should send a {@link Connection#noreplyWait()} before closing.
     * @return itself, once reconnected.
     */
    public @NotNull Connection reconnect(boolean noreplyWait) {
        try {
            return reconnectAsync(noreplyWait).join();
        } catch (CompletionException ce) {
            Throwable t = ce.getCause();
            if (t instanceof ReqlError) {
                throw ((ReqlError) t);
            }
            throw new ReqlDriverError(t);
        }
    }

    /**
     * Runs a ReQL query with options {@code optArgs}, the specified {@code fetchMode} and returns the result
     * asynchronously, with the values converted to the type of {@code TypeReference<T>}
     *
     * @param <T>       The type of result
     * @param term      The ReQL term
     * @param optArgs   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param unwrap    Override for the connection's unwrapLists setting
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public @NotNull <T> CompletableFuture<Result<T>> runAsync(@NotNull ReqlAst term,
                                                              @NotNull OptArgs optArgs,
                                                              @Nullable FetchMode fetchMode,
                                                              @Nullable Boolean unwrap,
                                                              @Nullable TypeReference<T> typeRef) {
        handleOptArgs(optArgs);
        Query q = Query.createStart(nextToken.incrementAndGet(), term, optArgs);
        if (optArgs.containsKey("noreply")) {
            throw new ReqlDriverError("Don't provide the noreply option as an optarg. Use `.runNoReply` instead of `.run`");
        }
        return runQuery(q, fetchMode, unwrap, typeRef);
    }

    /**
     * Runs a ReQL query with options {@code optArgs}, the specified {@code fetchMode} and returns the result, with the
     * values converted to the type of {@code TypeReference<T>}
     *
     * @param <T>       The type of result
     * @param term      The ReQL term
     * @param optArgs   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param unwrap    Override for the connection's unwrapLists setting
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public @NotNull <T> Result<T> run(@NotNull ReqlAst term,
                                      @NotNull OptArgs optArgs,
                                      @Nullable FetchMode fetchMode,
                                      @Nullable Boolean unwrap,
                                      @Nullable TypeReference<T> typeRef) {
        try {
            return runAsync(term, optArgs, fetchMode, unwrap, typeRef).join();
        } catch (CompletionException ce) {
            Throwable t = ce.getCause();
            if (t instanceof ReqlError) {
                throw ((ReqlError) t);
            }
            throw new ReqlDriverError(t);
        }
    }

    /**
     * Runs a <code>server_info</code> query to the server and returns the server info asynchronously.
     *
     * @return The server info.
     */
    public @NotNull CompletableFuture<Server> serverAsync() {
        return sendQuery(Query.createServerInfo(nextToken.incrementAndGet())).thenApply(res -> {
            if (res.type.equals(ResponseType.SERVER_INFO)) {
                return Internals.toPojo(res.data.get(0), Types.of(Server.class));
            }
            throw new ReqlDriverError("Did not receive a SERVER_INFO response.");
        });
    }

    /**
     * Runs a <code>server_info</code> query to the server and returns the server info.
     *
     * @return The server info.
     */
    public @NotNull Server server() {
        try {
            return serverAsync().join();
        } catch (CompletionException ce) {
            Throwable t = ce.getCause();
            if (t instanceof ReqlError) {
                throw ((ReqlError) t);
            }
            throw new ReqlDriverError(t);
        }
    }

    /**
     * Runs a <code>noreply_wait</code> query to the server and awaits it asynchronously.
     *
     * @return a {@link CompletableFuture} you can await.
     */
    public @NotNull CompletableFuture<Void> noreplyWaitAsync() {
        return runQuery(Query.createNoreplyWait(nextToken.incrementAndGet()), null, null, null).thenApply(ignored -> null);
    }

    /**
     * Runs a <code>noreply_wait</code> query to the server and awaits it.
     */
    public void noreplyWait() {
        try {
            noreplyWaitAsync().join();
        } catch (CompletionException ce) {
            Throwable t = ce.getCause();
            if (t instanceof ReqlError) {
                throw ((ReqlError) t);
            }
            throw new ReqlDriverError(t);
        }
    }

    /**
     * Runs this query via connection {@code conn} with options {@code optArgs} without awaiting the response.
     *
     * @param term    The ReQL term
     * @param optArgs The options to run this query with
     */
    public void runNoReply(@NotNull ReqlAst term, @NotNull OptArgs optArgs) {
        handleOptArgs(optArgs);
        optArgs.with("noreply", true);
        runQueryNoreply(Query.createStart(nextToken.incrementAndGet(), term, optArgs));
    }

    /**
     * Closes this connection asynchronously, closing all {@link Result}s, the {@link ResponsePump} and the {@link ConnectionSocket}.
     *
     * @return a {@link CompletableFuture} which completes when everything is closed.
     */
    public CompletableFuture<Void> closeAsync() {
        return closeAsync(true);
    }

    /**
     * Closes this connection asynchronously, closing all {@link Result}s, the {@link ResponsePump} and the {@link ConnectionSocket}.
     *
     * @param shouldNoreplyWait If the connection should noreply_wait before closing
     * @return a {@link CompletableFuture} which completes when everything is closed.
     */
    public CompletableFuture<Void> closeAsync(boolean shouldNoreplyWait) {
        return CompletableFuture.runAsync(() -> this.close(shouldNoreplyWait));
    }

    /**
     * Closes this connection, closing all {@link Result}s, the {@link ResponsePump} and the {@link ConnectionSocket}.
     */
    @Override
    public void close() {
        close(true);
    }

    /**
     * Closes this connection, closing all {@link Result}s, the {@link ResponsePump} and the {@link ConnectionSocket}.
     *
     * @param shouldNoreplyWait If the connection should noreply_wait before closing
     */
    public void close(boolean shouldNoreplyWait) {
        // disconnect
        try {
            if (shouldNoreplyWait) {
                noreplyWait();
            }
        } finally {
            // reset token
            nextToken.set(0);

            // clear cursor cache
            for (Result<?> handler : tracked) {
                handler.onConnectionClosed();
            }

            // terminate response pump
            if (pump != null) {
                pump.shutdownPump();
            }

            // close the socket
            if (socket != null) {
                socket.close();
            }
        }
    }

    /**
     * Closes all {@link Result}s currently opened.
     */
    public void closeResults() {
        for (Result<?> handler : tracked) {
            handler.close();
        }
    }

    // protected methods

    protected void sendStop(long token) {
        // While the server does reply to the stop request, we ignore that reply.
        // This works because the response pump in `connect` ignores replies for which
        // no waiter exists.
        runQueryNoreply(Query.createStop(token));
    }


    protected @NotNull CompletableFuture<Response> sendContinue(long token) {
        return sendQuery(Query.createContinue(token));
    }

    protected void loseTrackOf(@NotNull Result<?> r) {
        tracked.add(r);
    }

    protected void keepTrackOf(@NotNull Result<?> r) {
        tracked.remove(r);
    }

    /**
     * Writes a query and returns a completable future.
     * Said completable future value will eventually be set by the runnable response pump (see {@link #connect}).
     *
     * @param query the query to execute.
     * @return a completable future.
     */
    protected @NotNull CompletableFuture<Response> sendQuery(@NotNull Query query) {
        if (socket == null || !socket.isOpen()) {
            throw new ReqlDriverError("Client not connected.");
        }

        if (pump == null) {
            throw new ReqlDriverError("Response pump is not running.");
        }

        CompletableFuture<Response> response = pump.await(query.token);
        try {
            writeLock.lock();
            socket.write(query.serialize());
            return response;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Writes a query without waiting for a response
     *
     * @param query the query to execute.
     */
    protected void runQueryNoreply(@NotNull Query query) {
        if (socket == null || !socket.isOpen()) {
            throw new ReqlDriverError("Client not connected.");
        }

        if (pump == null) {
            throw new ReqlDriverError("Response pump is not running.");
        }

        try {
            writeLock.lock();
            socket.write(query.serialize());
        } finally {
            writeLock.unlock();
        }
    }

    protected @NotNull <T> CompletableFuture<Result<T>> runQuery(@NotNull Query query,
                                                                 @Nullable FetchMode fetchMode,
                                                                 @Nullable Boolean unwrap,
                                                                 @Nullable TypeReference<T> typeRef) {
        return sendQuery(query).thenApply(res -> new Result<>(
            this, query, res,
            fetchMode == null ? defaultFetchMode : fetchMode,
            unwrap == null ? unwrapLists : unwrap,
            typeRef));
    }

    protected void handleOptArgs(@NotNull OptArgs optArgs) {
        if (!optArgs.containsKey("db") && dbname != null) {
            // Only override the db global arg if the user hasn't
            // specified one already and one is specified on the connection
            optArgs.with("db", dbname);
        }
        if (optArgs.containsKey("db")) {
            // The db arg must be wrapped in a db ast object
            optArgs.with("db", new Db(Arguments.make(optArgs.get("db"))));
        }
    }

    // builder

    /**
     * Builder should be used to build a Connection instance.
     */
    public static class Builder {
        private @Nullable ConnectionSocket.Factory socketFactory;
        private @Nullable ResponsePump.Factory pumpFactory;
        private @Nullable String hostname;
        private @Nullable Integer port;
        private @Nullable String dbname;
        private @Nullable SSLContext sslContext;
        private @Nullable Long timeout;
        private @Nullable String authKey;
        private @Nullable String user;
        private @Nullable String password;
        private @Nullable FetchMode defaultFetchMode;
        private boolean unwrapLists = false;
        private boolean persistentThreads = false;

        /**
         * Creates an empty builder.
         */
        public Builder() {
        }

        /**
         * Parses a db-ul as a builder.
         *
         * @param uri the db-url to parse.
         */
        public Builder(@NotNull URI uri) {
            Objects.requireNonNull(uri, "URI can't be null. Use the default constructor instead.");
            if (!"rethinkdb".equals(uri.getScheme())) {
                throw new IllegalArgumentException("Schema of the URL is not 'rethinkdb'.");
            }

            String userInfo = uri.getUserInfo();
            String host = uri.getHost();
            int port = uri.getPort();
            String path = uri.getPath();
            String query = uri.getQuery();

            if (userInfo != null && !userInfo.isEmpty()) {
                String[] split = userInfo.split(":");
                if (split.length > 2) {
                    throw new IllegalArgumentException("Invalid user info: '" + userInfo + "'");
                }
                if (split.length > 0) {
                    this.user = split[0];
                }
                if (split.length > 1) {
                    this.password = split[1];
                }
            }
            if (host != null && !host.isEmpty()) {
                this.hostname = host.trim();
            }
            if (port != -1) {
                this.port = port;
            }
            if (path != null && !path.isEmpty()) {
                if (path.charAt(0) == '/') {
                    path = path.substring(1);
                }
                if (!path.isEmpty()) {
                    this.dbname = path;
                }
            }
            if (query != null) {
                String[] kvs = query.split("&");
                for (String kv : kvs) {
                    int i = kv.indexOf('=');
                    String k = i != -1 ? kv.substring(0, i) : kv;
                    String v = i != -1 ? kv.substring(i + 1) : "";
                    boolean booleanValue = v.isEmpty() || "true".equals(v) || "enabled".equals(v);
                    switch (k) {
                        case "auth_key":
                        case "authKey": {
                            String authKey = v;
                            if (authKey.isEmpty()) {
                                LOGGER.debug("Ignoring empty '{}'", v);
                                break;
                            }
                            if (authKey.charAt(0) == '\'' && authKey.charAt(authKey.length() - 1) == '\'') {
                                authKey = authKey.substring(1, authKey.length() - 1).replace("\\'", "'");
                            }
                            this.authKey = authKey;
                            break;
                        }
                        case "timeout": {
                            this.timeout = Long.parseLong(v);
                            break;
                        }
                        case "java.default_fetch_mode":
                        case "java.defaultFetchMode": {
                            this.defaultFetchMode = FetchMode.fromString(v);
                            break;
                        }
                        case "java.unwrap_lists":
                        case "java.unwrapLists": {
                            this.unwrapLists = booleanValue;
                            break;
                        }
                        case "java.persistent_threads":
                        case "java.persistentThreads": {
                            this.persistentThreads = booleanValue;
                            break;
                        }
                        default: {
                            LOGGER.debug("Invalid query parameter '{}', skipping", k);
                        }
                    }
                }
            }
        }

        /**
         * Copies a connection builder.
         *
         * @param b the original builder.
         */
        public Builder(Builder b) {
            socketFactory = b.socketFactory;
            pumpFactory = b.pumpFactory;
            hostname = b.hostname;
            port = b.port;
            dbname = b.dbname;
            sslContext = b.sslContext;
            timeout = b.timeout;
            authKey = b.authKey;
            user = b.user;
            password = b.password;
            unwrapLists = b.unwrapLists;
            defaultFetchMode = b.defaultFetchMode;
            persistentThreads = b.persistentThreads;
        }

        /**
         * Creates a copy of this builder.
         *
         * @return a copy of this builder.
         * @deprecated Use {@link com.rethinkdb.RethinkDB#connection(Builder) r.connection(Builder)} instead.
         * <b><i>(Will be removed on v2.5.0)</i></b>
         */
        @Deprecated
        public @NotNull Builder copyOf() {
            return new Builder(this);
        }

        public @NotNull Builder hostname(@Nullable String val) {
            hostname = val;
            return this;
        }

        public @NotNull Builder port(@Nullable Integer val) {
            port = val;
            return this;
        }

        public @NotNull Builder db(@Nullable String val) {
            dbname = val;
            return this;
        }

        public @NotNull Builder authKey(@Nullable String key) {
            authKey = key;
            return this;
        }

        public @NotNull Builder user(@Nullable String user, @Nullable String password) {
            this.user = user;
            this.password = password;
            return this;
        }

        public @NotNull Builder timeout(@Nullable Long val) {
            timeout = val;
            return this;
        }

        public @NotNull Builder certFile(@NotNull File val) {
            try (InputStream stream = new FileInputStream(val)) {
                return sslContext(Internals.readCertFile(stream));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public @NotNull Builder certFile(@NotNull InputStream val) {
            try (InputStream stream = val) {
                return sslContext(Internals.readCertFile(stream));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public @NotNull Builder sslContext(@Nullable SSLContext val) {
            sslContext = val;
            return this;
        }

        /**
         * Sets a custom {@link ConnectionSocket} factory for the connection.
         * <p><i><b>(Java Driver-specific, No db-url support)</b></i></p>
         *
         * @param factory the connection socket factory, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder socketFactory(@Nullable ConnectionSocket.Factory factory) {
            socketFactory = factory;
            return this;
        }

        /**
         * Sets a custom {@link ResponsePump} factory for the connection.
         * <p><i><b>(Java Driver-specific, No db-url support)</b></i></p>
         *
         * @param factory the response pump factory, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder pumpFactory(@Nullable ResponsePump.Factory factory) {
            pumpFactory = factory;
            return this;
        }

        /**
         * Sets list unwrapping behaviour for lists.
         *
         * <blockquote>
         * <b>List unwrapping is a Java-driver specific behaviour that unwraps an atom response from the server,
         * which is a list, as if it were a sequence of objects.</b>
         * <p>
         * Consider the following:
         * <code>{@link TopLevel#expr(Object) r.expr}({@link TopLevel#array(Object, Object...) r.array("a", "b", "c")}).{@link com.rethinkdb.gen.ast.ReqlExpr#run(Connection) run(conn)};</code>
         * <p>
         * By default, it returns a {@link Result} with a single {@link List}["a","b","c"] inside.
         * <p>
         * With <b>list unwrapping</b>, it returns a {@link Result} with "a", "b", "c" inside.
         * <p>
         * The feature makes the code a bit less verbose. For example, iterating goes from:
         * <p>
         * <code>(({@code List<String>}) {@link Result result}{@link Result#single() .single()}){@link List#forEach(Consumer) .forEach(s -> ...)}</code>
         * <p>
         * To:
         * <p>
         * <code>{@link Result result}{@link Result#forEach(Consumer) .forEach(s -> ...)}</code>
         * </blockquote>
         *
         * <p><i><b>(Java Driver-specific, Db-url key: </b></i><code>"java.unwrap_lists"</code><i><b>)</b></i></p>
         *
         * @param val {@code true} to enable list unwrapping, {@code false} to disable.
         * @return itself.
         */
        public @NotNull Builder unwrapLists(boolean val) {
            unwrapLists = val;
            return this;
        }

        /**
         * Sets the default fetch mode for sequences.
         *
         * <blockquote>
         * <b>Fetch mode is a Java-driver specific behaviour that allows for fine-tuning on partial sequence fetching.</b>
         * <p>
         * Can be used to balance between high availability and network optimization. The
         * {@linkplain FetchMode#AGGRESSIVE aggressive} fetch mode will make best effort to consume the entire sequence, as
         * fast as possible, to ensure high availability to the consumer, while the {@linkplain FetchMode#LAZY lazy}
         * fetch mode will make no effort and await until all objects were consumed before fetching the next one.<br>
         * In addiction, there are many preemptive fetch modes, which will consume the next sequence once the buffer
         * reaches {@linkplain FetchMode#PREEMPTIVE_HALF half}, a {@linkplain FetchMode#PREEMPTIVE_THIRD third},
         * a {@linkplain FetchMode#PREEMPTIVE_FOURTH fourth}, a {@linkplain FetchMode#PREEMPTIVE_FIFTH fitfh},
         * a {@linkplain FetchMode#PREEMPTIVE_SIXTH sixth}, a {@linkplain FetchMode#PREEMPTIVE_SEVENTH seventh} or
         * an {@linkplain FetchMode#PREEMPTIVE_EIGHTH eighth} of it's capacity.
         * </blockquote>
         *
         * <p><i><b>(Java Driver-specific, Db-url key: </b></i><code>"java.default_fetch_mode"</code><i><b>)</b></i></p>
         *
         * @param val a default fetch mode, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder defaultFetchMode(@Nullable FetchMode val) {
            defaultFetchMode = val;
            return this;
        }

        /**
         * Sets if the response pump should use {@linkplain Thread#setDaemon(boolean) daemon threads} or not.<br>
         * <blockquote>
         * Using persistent threads guarantees that the JVM will not exit once the main thread finishes, but will keep
         * the JVM alive if the connection is not closed.<br>
         * Daemon threads will ensure that the JVM can exit automatically, but may or may not ignore ongoing
         * asynchronous queries. <i>Your milage may vary.</i> (HTTP or other application frameworks may open persistent
         * threads by themselves and may keep the JVM alive until the main window or application shuts down.)
         * </blockquote>
         *
         * <p><i><b>(Java Driver-specific, Db-url key: </b></i><code>"java.default_fetch_mode"</code><i><b>)</b></i></p>
         *
         * @param val {@code true} to use persistent threads in the response pump, {@code false} to use daemon threads.
         * @return itself.
         * @see Thread#setDaemon(boolean)
         */
        public @NotNull Builder persistentThreads(boolean val) {
            persistentThreads = val;
            return this;
        }

        public @NotNull CompletableFuture<Connection> connectAsync() {
            return new Connection(this).connectAsync();
        }

        public @NotNull Connection connect() {
            return new Connection(this).connect();
        }

        public @NotNull URI dbUrl() {
            return URI.create(dbUrlString());
        }

        public @NotNull String dbUrlString() {
            StringBuilder b = new StringBuilder("rethinkdb://");

            if (user != null) {
                b.append(user);
                if (password != null) {
                    b.append(':').append(password);
                }
                b.append('@');
            }

            b.append(hostname != null ? hostname : "127.0.0.1");

            if (port != null) {
                b.append(':').append(port);
            }

            if (dbname != null) {
                b.append('/').append(dbname);
            }

            boolean first = true;
            if (authKey != null) {
                first = false;
                b.append('?');

                b.append("auth_key=").append(authKey);
            }
            if (timeout != null) {
                b.append(first ? '?' : "&");
                first = false;

                b.append("timeout=").append(timeout);
            }
            if (defaultFetchMode != null) {
                b.append(first ? '?' : "&");
                first = false;

                b.append("java.default_fetch_mode=").append(defaultFetchMode.name().toLowerCase());
            }
            if (unwrapLists) {
                b.append(first ? '?' : "&");
                first = false;

                b.append("java.unwrap_lists=true");
            }
            if (persistentThreads) {
                b.append(first ? '?' : "&");

                first = false;
                b.append("java.persistent_threads=true");
            }

            return b.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Builder builder = (Builder) o;
            return unwrapLists == builder.unwrapLists &&
                Objects.equals(socketFactory, builder.socketFactory) &&
                Objects.equals(pumpFactory, builder.pumpFactory) &&
                Objects.equals(hostname, builder.hostname) &&
                Objects.equals(port, builder.port) &&
                Objects.equals(dbname, builder.dbname) &&
                Objects.equals(sslContext, builder.sslContext) &&
                Objects.equals(timeout, builder.timeout) &&
                Objects.equals(authKey, builder.authKey) &&
                Objects.equals(user, builder.user) &&
                Objects.equals(password, builder.password) &&
                defaultFetchMode == builder.defaultFetchMode &&
                persistentThreads == builder.persistentThreads;
        }

        @Override
        public int hashCode() {
            return Objects.hash(socketFactory, pumpFactory, hostname, port, dbname,
                sslContext, timeout, authKey, user, password, defaultFetchMode, unwrapLists, persistentThreads);
        }

        @Override
        public String toString() {
            return "Builder{" + dbUrlString() + '}';
        }
    }
}
