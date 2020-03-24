package com.rethinkdb.net;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.ast.Query;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.Db;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.proto.ResponseType;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.model.Server;
import com.rethinkdb.utils.Internals;
import com.rethinkdb.utils.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.net.URI;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    protected final boolean unwrapLists;
    protected final Result.FetchMode defaultFetchMode;

    protected final AtomicLong nextToken = new AtomicLong();
    protected final Set<Result<?>> tracked = ConcurrentHashMap.newKeySet();
    protected final Lock writeLock = new ReentrantLock();

    protected @Nullable String dbname;
    protected @Nullable ConnectionSocket socket;
    protected @Nullable ResponsePump pump;

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
        this.defaultFetchMode = b.defaultFetchMode != null ? b.defaultFetchMode : Result.FetchMode.LAZY;
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
     * Begins the socket connection to the server.
     *
     * @return itself, once connected.
     */
    public @NotNull Connection connect() {
        if (socket != null) {
            throw new ReqlDriverError("Client already connected!");
        }
        ConnectionSocket socket = socketFactory.newSocket(hostname, port, sslContext, timeout);
        this.socket = socket;
        HandshakeProtocol.doHandshake(socket, user, password, timeout);
        pump = pumpFactory.newPump(socket);
        return this;
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
        close(noreplyWait);
        connect();
        return this;
    }

    /**
     * Runs a ReQL query with options {@code optArgs}, the specified {@code fetchMode} and returns the result
     * asynchronously, with the values converted to the type of {@code TypeReference<T>}
     *
     * @param <T>       The type of result
     * @param term      The ReQL term
     * @param optArgs   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public @NotNull <T> CompletableFuture<Result<T>> runAsync(@NotNull ReqlAst term,
                                                              @NotNull OptArgs optArgs,
                                                              @Nullable Result.FetchMode fetchMode,
                                                              @Nullable TypeReference<T> typeRef) {
        handleOptArgs(optArgs);
        Query q = Query.createStart(nextToken.incrementAndGet(), term, optArgs);
        if (optArgs.containsKey("noreply")) {
            throw new ReqlDriverError("Don't provide the noreply option as an optarg. Use `.runNoReply` instead of `.run`");
        }
        return runQuery(q, fetchMode, typeRef);
    }

    /**
     * Runs a ReQL query with options {@code optArgs}, the specified {@code fetchMode} and returns the result, with the
     * values converted to the type of {@code TypeReference<T>}
     *
     * @param <T>       The type of result
     * @param term      The ReQL term
     * @param optArgs   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public @NotNull <T> Result<T> run(@NotNull ReqlAst term,
                                      @NotNull OptArgs optArgs,
                                      @Nullable Result.FetchMode fetchMode,
                                      @Nullable TypeReference<T> typeRef) {
        try {
            return runAsync(term, optArgs, fetchMode, typeRef).join();
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
        return runQuery(Query.createNoreplyWait(nextToken.incrementAndGet()), null, null).thenApply(ignored -> null);
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
                                                                 @Nullable Result.FetchMode fetchMode,
                                                                 @Nullable TypeReference<T> typeRef) {
        return sendQuery(query).thenApply(res -> new Result<>(
            this, query, res, fetchMode == null ? defaultFetchMode : fetchMode, typeRef
        ));
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
        private @Nullable Result.FetchMode defaultFetchMode;
        private boolean unwrapLists = false;

        public Builder() {
        }

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
            if (path != null) {
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
                            this.defaultFetchMode = Result.FetchMode.fromString(v);
                            break;
                        }
                        case "java.unwrap_lists":
                        case "java.unwrapLists": {
                            this.unwrapLists = v.isEmpty() || "true".equals(v) || "enabled".equals(v);
                            break;
                        }
                        default: {
                            LOGGER.debug("Invalid query parameter '{}', skipping", k);
                        }
                    }
                }
            }
        }

        public @NotNull Builder copyOf() {
            Builder c = new Builder();
            c.socketFactory = socketFactory;
            c.pumpFactory = pumpFactory;
            c.hostname = hostname;
            c.port = port;
            c.dbname = dbname;
            c.sslContext = sslContext;
            c.timeout = timeout;
            c.authKey = authKey;
            c.user = user;
            c.password = password;
            c.unwrapLists = unwrapLists;
            c.defaultFetchMode = defaultFetchMode;
            return c;
        }

        public @NotNull Builder socketFactory(@Nullable ConnectionSocket.Factory factory) {
            socketFactory = factory;
            return this;
        }

        public @NotNull Builder pumpFactory(@Nullable ResponsePump.Factory factory) {
            pumpFactory = factory;
            return this;
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

        public @NotNull Builder unwrapLists(boolean val) {
            unwrapLists = val;
            return this;
        }

        public @NotNull Builder defaultFetchMode(@Nullable Result.FetchMode val) {
            defaultFetchMode = val;
            return this;
        }

        public @NotNull Builder timeout(@Nullable Long val) {
            timeout = val;
            return this;
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
                defaultFetchMode == builder.defaultFetchMode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(socketFactory, pumpFactory, hostname, port, dbname,
                sslContext, timeout, authKey, user, password, defaultFetchMode, unwrapLists);
        }
    }
}
