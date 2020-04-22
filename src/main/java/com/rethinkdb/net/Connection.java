package com.rethinkdb.net;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.ast.Query;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.Db;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.model.TopLevel;
import com.rethinkdb.gen.proto.ResponseType;
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
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
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
    private static final @NotNull Logger LOGGER = LoggerFactory.getLogger(Connection.class);

    protected final @NotNull String hostname;
    protected final int port;
    protected final @Nullable String user;
    protected final @Nullable String password;
    protected final @Nullable Long timeout;
    protected final @Nullable SSLContext sslContext;
    //java-only
    protected final @NotNull ConnectionSocket.Factory socketFactory;
    protected final @NotNull ResponsePump.Factory pumpFactory;
    protected final @NotNull FetchMode defaultFetchMode;
    protected final boolean unwrapLists;
    protected final boolean persistentThreads;

    protected final @NotNull AtomicLong nextToken = new AtomicLong();
    protected final @NotNull Set<Result<?>> tracked = ConcurrentHashMap.newKeySet();
    protected final @NotNull Lock writeLock = new ReentrantLock();

    protected @Nullable String dbname;
    protected @Nullable ConnectionSocket socket;
    protected @Nullable ResponsePump pump;

    /**
     * Creates a new connection based on a {@link Builder}.
     *
     * @param b the connection builder
     */
    public Connection(@NotNull Builder b) {
        this.hostname = b.hostname != null ? b.hostname : "127.0.0.1";
        this.port = b.port != null ? b.port : 28015;
        this.user = b.user != null ? b.user : "admin";
        this.password = b.password != null ? b.password : "";
        this.dbname = b.dbname;
        this.timeout = b.timeout;
        this.sslContext = b.sslContext;
        //java-only
        this.socketFactory = b.socketFactory != null ? b.socketFactory : DefaultConnectionFactory.INSTANCE;
        this.pumpFactory = b.pumpFactory != null ? b.pumpFactory : DefaultConnectionFactory.INSTANCE;
        this.unwrapLists = b.unwrapLists;
        this.defaultFetchMode = b.defaultFetchMode != null ? b.defaultFetchMode : FetchMode.LAZY;
        this.persistentThreads = b.persistentThreads;
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
        return createSocketAsync().thenApply(socket -> {
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
    public @NotNull CompletableFuture<Void> closeAsync() {
        return closeAsync(true);
    }

    /**
     * Closes this connection asynchronously, closing all {@link Result}s, the {@link ResponsePump} and the {@link ConnectionSocket}.
     *
     * @param shouldNoreplyWait If the connection should noreply_wait before closing
     * @return a {@link CompletableFuture} which completes when everything is closed.
     */
    public @NotNull CompletableFuture<Void> closeAsync(boolean shouldNoreplyWait) {
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

    /**
     * Checks if there are any ongoing query.
     * @return {@code true} if there's an ongoing query that will be closed if this connection is closed.
     */
    public boolean hasOngoingQueries() {
        return !tracked.isEmpty();
    }

    // protected methods

    /**
     * Sends a STOP query. Used by {@link Result} partial sequences.
     *
     * @param token the response token.
     */
    protected void sendStop(long token) {
        // While the server does reply to the stop request, we ignore that reply.
        // This works because the response pump in `connect` ignores replies for which
        // no waiter exists.
        runQueryNoreply(Query.createStop(token));
    }

    /**
     * Sends a CONTINUE query. Used by {@link Result} partial sequences.
     *
     * @param token the response token.
     * @return a completable future which completes with the next response.
     */
    protected @NotNull CompletableFuture<Response> sendContinue(long token) {
        return sendQuery(Query.createContinue(token));
    }

    /**
     * Callback method from {@link Result}, signals that this connection should keep track of this result.
     * Tracked results can be closed using {@link Connection#close()} or {@link Connection#closeResults()}.
     *
     * @param r the result to be tracked.
     */
    protected void keepTrackOf(@NotNull Result<?> r) {
        tracked.add(r);
    }

    /**
     * Callback method from {@link Result}, signals that this connection should no long keep track of this result.
     * The result probably finished or was closed.
     *
     * @param r the result to be tracked.
     */
    protected void loseTrackOf(@NotNull Result<?> r) {
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

    /**
     * Sents a query to the server and returns a {@link CompletableFuture} which completes with a result.
     * Runs a ReQL query with options {@code optArgs}, the specified {@code fetchMode} and returns the result, with the
     * values converted to the type of {@code TypeReference<T>}
     *
     * @param <T>       The type of result
     * @param query     The query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param unwrap    Override for the connection's unwrapLists setting
     * @param typeRef   The type to convert to
     * @return The {@link CompletableFuture} which completes with the result of the query.
     */
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

    /**
     * Handle optArgs before sending to the server.
     *
     * @param optArgs the optArgs.
     */
    protected void handleOptArgs(@NotNull OptArgs optArgs) {
        if (optArgs.containsKey("db")) {
            // The db arg must be wrapped in a db ast object
            optArgs.with("db", new Db(optArgs.get("db")));
        } else if (dbname != null) {
            // Only override the db global arg if the user hasn't
            // specified one already and one is specified on the connection
            optArgs.with("db", new Db(dbname));
        }
    }

    /**
     * Detects if the connection socket supports async creation or wraps it before returning.
     *
     * @return a {@link CompletableFuture} which will complete with a new {@link ConnectionSocket}.
     */
    protected @NotNull CompletableFuture<ConnectionSocket> createSocketAsync() {
        if (socketFactory instanceof ConnectionSocket.AsyncFactory) {
            return ((ConnectionSocket.AsyncFactory) socketFactory).newSocketAsync(hostname, port, sslContext, timeout);
        }
        return CompletableFuture.supplyAsync(() -> socketFactory.newSocket(hostname, port, sslContext, timeout));
    }

    // builder

    /**
     * Builder should be used to build a Connection instance.
     */
    public static class Builder {
        private @Nullable String hostname;
        private @Nullable Integer port;
        private @Nullable String user;
        private @Nullable String password;
        private @Nullable String dbname;
        private @Nullable Long timeout;
        private @Nullable SSLContext sslContext;
        // java-only
        private @Nullable ConnectionSocket.Factory socketFactory;
        private @Nullable ResponsePump.Factory pumpFactory;
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

        /**
         * Copies a connection builder.
         *
         * @param b the original builder.
         */
        public Builder(@NotNull Builder b) {
            hostname = b.hostname;
            port = b.port;
            user = b.user;
            password = b.password;
            dbname = b.dbname;
            timeout = b.timeout;
            sslContext = b.sslContext;
            // java-only
            socketFactory = b.socketFactory;
            pumpFactory = b.pumpFactory;
            unwrapLists = b.unwrapLists;
            defaultFetchMode = b.defaultFetchMode;
            persistentThreads = b.persistentThreads;
        }

        /**
         * Sets a custom hostname for the connection.
         * <p>(Configurable by Db-url)</p>
         *
         * @param hostname the hostname, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder hostname(@Nullable String hostname) {
            this.hostname = hostname;
            return this;
        }

        /**
         * Sets a custom port for the connection.
         * <p>(Configurable by Db-url)</p>
         *
         * @param port the port, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder port(@Nullable Integer port) {
            this.port = port;
            return this;
        }

        /**
         * Sets a custom username for the connection.
         * <p>(Configurable by Db-url)</p>
         *
         * @param user the username, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder user(@Nullable String user) {
            this.user = user;
            return this;
        }

        /**
         * Sets a custom username and password for the connection.
         * <p>(Configurable by Db-url)</p>
         *
         * @param user     the username, or {@code null}.
         * @param password the password, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder user(@Nullable String user, @Nullable String password) {
            this.user = user;
            this.password = password;
            return this;
        }

        /**
         * Sets a custom database for the connection.
         * <p>(Configurable by Db-url)</p>
         *
         * @param dbname the database name, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder db(@Nullable String dbname) {
            this.dbname = dbname;
            return this;
        }

        /**
         * Sets a custom timeout for the connection.
         * <p>(Db-url key: <code>"timeout"</code>)</p>
         *
         * @param timeout the timeout, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder timeout(@Nullable Long timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Sets a custom authentication key for the connection.
         * <p><i><b>(No db-url support)</b></i></p>
         *
         * @param authKey the authentication key, or {@code null}.
         * @return itself.
         * @deprecated Use {@link Builder#user(String, String)} instead.
         * <b><i>(Will be removed on v2.5.0)</i></b>
         */
        @Deprecated
        public @NotNull Builder authKey(@Nullable String authKey) {
            return user(null, authKey);
        }

        /**
         * Sets a certificate to provide SSL encryption to the RethinkDB Connection.
         * <p><i><b>(No db-url support)</b></i></p>
         *
         * @param source a callable which provides a {@link InputStream} with the contents of a certificate file.
         * @return itself.
         */
        public @NotNull Builder certFile(@NotNull Callable<InputStream> source) {
            try (InputStream stream = source.call()) {
                return sslContext(Internals.readCertFile(stream));
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Sets a certificate to provide SSL encryption to the RethinkDB Connection.
         * <p><i><b>(No db-url support)</b></i></p>
         *
         * @param source a {@link InputStream} with the contents of a certificate file.
         * @return itself.
         */
        public @NotNull Builder certFile(@NotNull InputStream source) {
            return certFile(() -> source);
        }

        /**
         * Sets a certificate to provide SSL encryption to the RethinkDB Connection.
         * <p><i><b>(No db-url support)</b></i></p>
         *
         * @param file a certificate file to read from.
         * @return itself.
         */
        public @NotNull Builder certFile(@NotNull File file) {
            return certFile(() -> new FileInputStream(file));
        }

        /**
         * Sets a {@link SSLContext} to provide SSL encryption to the RethinkDB Connection.
         * <p><i><b>(No db-url support)</b></i></p>
         *
         * @param sslContext the SSL context, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder sslContext(@Nullable SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        /**
         * Sets a custom {@link ConnectionSocket} factory for the connection.
         * <p><i><b>(Java Driver-specific, No db-url support)</b></i></p>
         *
         * @param socketFactory the connection socket factory, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder socketFactory(@Nullable ConnectionSocket.Factory socketFactory) {
            this.socketFactory = socketFactory;
            return this;
        }

        /**
         * Sets a custom {@link ResponsePump} factory for the connection.
         * <p><i><b>(Java Driver-specific, No db-url support)</b></i></p>
         *
         * @param pumpFactory the response pump factory, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder pumpFactory(@Nullable ResponsePump.Factory pumpFactory) {
            this.pumpFactory = pumpFactory;
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
         * @param defaultFetchMode a default fetch mode, or {@code null}.
         * @return itself.
         */
        public @NotNull Builder defaultFetchMode(@Nullable FetchMode defaultFetchMode) {
            this.defaultFetchMode = defaultFetchMode;
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
         * @param enabled {@code true} to enable list unwrapping, {@code false} to disable.
         * @return itself.
         */
        public @NotNull Builder unwrapLists(boolean enabled) {
            unwrapLists = enabled;
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
         * @param enabled {@code true} to use persistent threads in the response pump, {@code false} to use daemon threads.
         * @return itself.
         * @see Thread#setDaemon(boolean)
         */
        public @NotNull Builder persistentThreads(boolean enabled) {
            persistentThreads = enabled;
            return this;
        }

        /**
         * Creates a new connection and connects asynchronously to the server.
         *
         * @return a {@link CompletableFuture} which completes with the connection once connected.
         */
        public @NotNull CompletableFuture<Connection> connectAsync() {
            return new Connection(this).connectAsync();
        }

        /**
         * Creates a new connection and connects to the server.
         *
         * @return a newly created connection, connected to the server.
         */
        public @NotNull Connection connect() {
            return new Connection(this).connect();
        }

        /**
         * Creates a {@link URI} with the db-url representing this connection builder.
         *
         * @return the db-url as a {@link URI}.
         */
        public @NotNull URI dbUrl() {
            return URI.create(dbUrlString());
        }

        /**
         * Creates a db-url representing this connection builder.
         *
         * @return the db-url.
         */
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
            if (timeout != null) {
                b.append('?');
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
            return Objects.equals(hostname, builder.hostname) &&
                Objects.equals(port, builder.port) &&
                Objects.equals(user, builder.user) &&
                Objects.equals(password, builder.password) &&
                Objects.equals(dbname, builder.dbname) &&
                Objects.equals(timeout, builder.timeout) &&
                Objects.equals(sslContext, builder.sslContext) &&
                Objects.equals(socketFactory, builder.socketFactory) &&
                Objects.equals(pumpFactory, builder.pumpFactory) &&
                Objects.equals(defaultFetchMode, builder.defaultFetchMode) &&
                unwrapLists == builder.unwrapLists &&
                persistentThreads == builder.persistentThreads;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return Objects.hash(
                hostname, port, user, password, dbname, timeout, sslContext,
                socketFactory, pumpFactory, defaultFetchMode, unwrapLists, persistentThreads
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "Builder{" + dbUrlString() + '}';
        }
    }
}
