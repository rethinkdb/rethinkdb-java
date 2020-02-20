package com.rethinkdb.net;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.ast.Query;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.Db;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.model.Server;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
        this.hostname = b.hostname != null ? b.hostname : "localhost";
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
     */
    public void use(@Nullable String db) {
        dbname = db;
    }

    /**
     * Checks if the connection is open.
     *
     * @return true if the socket and the response pump are working, otherwise false.
     */
    public boolean isOpen() {
        return socket != null && socket.isOpen() && pump != null && pump.isOpen();
    }

    /**
     * Begins the socket connection to the server.
     *
     * @return itself, once connected.
     */
    public Connection connect() {
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
    public Connection reconnect() {
        return reconnect(false);
    }

    /**
     * Closes and reconnects to the server.
     *
     * @param noreplyWait if closing should send a {@link Connection#noreplyWait()} before closing.
     * @return itself, once reconnected.
     */
    public Connection reconnect(boolean noreplyWait) {
        close(noreplyWait);
        connect();
        return this;
    }

    public <T> CompletableFuture<Result<T>> runAsync(ReqlAst term,
                                                     OptArgs optArgs,
                                                     @Nullable Result.FetchMode fetchMode,
                                                     @Nullable TypeReference<T> typeRef) {
        handleOptArgs(optArgs);
        Query q = Query.createStart(nextToken.incrementAndGet(), term, optArgs);
        if (optArgs.containsKey("noreply")) {
            throw new ReqlDriverError("Don't provide the noreply option as an optarg. Use `.runNoReply` instead of `.run`");
        }
        return runQuery(q, fetchMode, typeRef);
    }

    public <T> Result<T> run(ReqlAst term,
                             OptArgs optArgs,
                             @Nullable Result.FetchMode fetchMode,
                             @Nullable TypeReference<T> typeRef) {
        return runAsync(term, optArgs, fetchMode, typeRef).join();
    }

    public CompletableFuture<Server> serverAsync() {
        return sendQuery(Query.createServerInfo(nextToken.incrementAndGet())).thenApply(res -> {
            if (res.isServerInfo()) {
                return Util.convertToPojo(res.data.get(0), new TypeReference<Server>() {});
            }
            throw new ReqlDriverError("Did not receive a SERVER_INFO response.");
        });
    }

    public Server server() {
        return serverAsync().join();
    }

    public CompletableFuture<Void> noreplyWaitAsync() {
        return runQuery(Query.createNoreplyWait(nextToken.incrementAndGet()), null, null).thenApply(ignored -> null);
    }

    public void noreplyWait() {
        noreplyWaitAsync().join();
    }

    public void runNoReply(ReqlAst term, OptArgs optArgs) {
        handleOptArgs(optArgs);
        optArgs.with("noreply", true);
        runQueryNoreply(Query.createStart(nextToken.incrementAndGet(), term, optArgs));
    }

    @Override
    public void close() {
        close(false);
    }

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

    public void closeResults() {
        for (Result<?> handler : tracked) {
            handler.close();
        }
    }

    // package-private methods

    protected void sendStop(long token) {
        // While the server does reply to the stop request, we ignore that reply.
        // This works because the response pump in `connect` ignores replies for which
        // no waiter exists.
        runQueryNoreply(Query.createStop(token));
    }

    protected CompletableFuture<Response> sendContinue(long token) {
        return sendQuery(Query.createContinue(token));
    }

    protected void loseTrackOf(Result<?> r) {
        tracked.add(r);
    }

    protected void keepTrackOf(Result<?> r) {
        tracked.remove(r);
    }

    // private methods

    /**
     * Writes a query and returns a completable future.
     * Said completable future value will eventually be set by the runnable response pump (see {@link #connect}).
     *
     * @param query the query to execute.
     * @return a completable future.
     */
    protected CompletableFuture<Response> sendQuery(Query query) {
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
    protected void runQueryNoreply(Query query) {
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

    protected <T> CompletableFuture<Result<T>> runQuery(Query query,
                                                        @Nullable Result.FetchMode fetchMode,
                                                        @Nullable TypeReference<T> typeRef) {
        return sendQuery(query).thenApply(res -> new Result<>(
            this, query, res, fetchMode == null ? defaultFetchMode : fetchMode, typeRef
        ));
    }

    protected void handleOptArgs(OptArgs optArgs) {
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
                    throw new IllegalArgumentException("Invalid user info.");
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
                    String[] split = kv.split("=");
                    if (split.length != 2) {
                        throw new IllegalArgumentException("Invalid query.");
                    }
                    switch (split[0]) {
                        case "auth_key": {
                            String authKey = split[1];
                            if (authKey.isEmpty()) {
                                throw new IllegalArgumentException("Invalid query value.");
                            }
                            if (authKey.charAt(0) == '\'' && authKey.charAt(authKey.length() - 1) == '\'') {
                                authKey = authKey.substring(1, authKey.length() - 1).replace("\\'", "'");
                            }
                            this.authKey = authKey;
                            break;
                        }
                        case "timeout": {
                            this.timeout = Long.parseLong(split[1]);
                            break;
                        }
                        default: {
                            throw new IllegalArgumentException("Invalid query parameter.");
                        }
                    }
                }
            }
        }

        public Builder copyOf() {
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

        public Builder socketFactory(ConnectionSocket.Factory factory) {
            socketFactory = factory;
            return this;
        }

        public Builder pumpFactory(ResponsePump.Factory factory) {
            pumpFactory = factory;
            return this;
        }

        public Builder hostname(String val) {
            hostname = val;
            return this;
        }

        public Builder port(int val) {
            port = val;
            return this;
        }

        public Builder db(String val) {
            dbname = val;
            return this;
        }

        public Builder authKey(String key) {
            authKey = key;
            return this;
        }

        public Builder user(String user, String password) {
            this.user = user;
            this.password = password;
            return this;
        }

        public Builder certFile(InputStream val) {
            sslContext = Crypto.readCertFile(val);
            return this;
        }

        public Builder sslContext(SSLContext val) {
            sslContext = val;
            return this;
        }

        public Builder unwrapLists(boolean val) {
            unwrapLists = val;
            return this;
        }

        public Builder defaultFetchMode(Result.FetchMode val) {
            defaultFetchMode = val;
            return this;
        }

        public Builder timeout(long val) {
            timeout = val;
            return this;
        }

        public Connection connect() {
            final Connection conn = new Connection(this);
            conn.reconnect();
            return conn;
        }
    }
}
