package com.rethinkdb.net;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.ast.Query;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.ast.Db;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.OptArgs;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Connection implements Closeable {
    // public immutable
    public final String hostname;
    public final int port;

    private final AtomicLong nextToken = new AtomicLong();

    // private mutable
    private @Nullable String dbname;
    private @Nullable Long connectTimeout;
    private @Nullable SSLContext sslContext;
    private final Handshake handshake;

    // network stuff
    private @Nullable SocketWrapper socket;


    // execution stuff
    private ExecutorService exec;
    private final Map<Long, CompletableFuture<Response>> awaiters = new ConcurrentHashMap<>();
    private Exception awaiterException = null;
    private final ReentrantLock lock = new ReentrantLock();

    public Connection(Builder builder) {
        dbname = builder.dbname;
        if (builder.authKey != null && builder.user != null) {
            throw new ReqlDriverError("Either `authKey` or `user` can be used, but not both.");
        }
        String user = builder.user != null ? builder.user : "admin";
        String password = builder.password != null ? builder.password : builder.authKey != null ? builder.authKey : "";
        handshake = new Handshake(user, password);
        hostname = builder.hostname != null ? builder.hostname : "localhost";
        port = builder.port != null ? builder.port : 28015;
        // is certFile provided? if so, it has precedence over SSLContext
        sslContext = Crypto.handleCertfile(builder.certFile, builder.sslContext);
        connectTimeout = builder.timeout;
    }

    public @Nullable String db() {
        return dbname;
    }

    public void connect() {
        connect(null);
    }

    public Connection reconnect() {
        return reconnect(false, null);
    }

    public Connection reconnect(boolean noreplyWait, @Nullable Long timeout) {
        if (timeout == null) {
            timeout = connectTimeout;
        }
        close(noreplyWait);
        connect(timeout);
        return this;
    }

    private void connect(@Nullable Long timeout) {
        final SocketWrapper sock = new SocketWrapper(hostname, port, sslContext, timeout != null ? timeout : connectTimeout);
        sock.connect(handshake);
        socket = sock;

        // start response pump
        exec = Executors.newSingleThreadExecutor();
        exec.submit(() -> {
            // pump responses until canceled
            while (true) {
                // validate socket is open
                if (!isOpen()) {
                    awaiterException = new IOException("The socket is closed, exiting response pump.");
                    this.close();
                    break;
                }

                // read response and send it to whoever is waiting, if anyone
                try {
                    if (socket == null) {
                        throw new ReqlDriverError("No socket available.");
                    }
                    final Response response = socket.read();
                    final CompletableFuture<Response> awaiter = awaiters.remove(response.token);
                    if (awaiter != null) {
                        awaiter.complete(response);
                    }
                } catch (Exception e) {
                    awaiterException = e;
                    this.close();
                    break;
                }
            }
        });
    }

    @Nullable
    public Integer clientPort() {
        if (socket != null) {
            return socket.clientPort();
        }
        return null;
    }

    @Nullable
    public SocketAddress clientAddress() {
        if (socket != null) {
            return socket.clientAddress();
        }
        return null;
    }

    public boolean isOpen() {
        if (socket != null) {
            return socket.isOpen();
        }
        return false;
    }

    @Override
    public void close() {
        close(false);
    }

    public void close(boolean shouldNoreplyWait) {
        // disconnect
        try {
            if (shouldNoreplyWait) {
                runQuery(Query.noreplyWait(newToken()), null);
            }
        } finally {
            // reset token
            nextToken.set(0);

            // clear cursor cache
            for (ResponseHandler<?> handler : tracked) {
                try {
                    handler.onConnectionClosed();
                } catch (InterruptedException ignored) {
                }
            }

            // handle current awaiters
            this.awaiters.values().forEach(awaiter -> {
                // what happened?
                if (this.awaiterException != null) { // an exception
                    awaiter.completeExceptionally(this.awaiterException);
                } else { // probably canceled
                    awaiter.cancel(true);
                }
            });
            awaiters.clear();

            // terminate response pump
            if (exec != null && !exec.isShutdown()) {
                exec.shutdown();
            }

            // close the socket
            if (socket != null) {
                socket.close();
            }
        }
    }

    public void use(String db) {
        dbname = db;
    }

    public @Nullable Long timeout() {
        return connectTimeout;
    }

    /**
     * Writes a query and returns a completable future.
     * Said completable future value will eventually be set by the runnable response pump (see {@link #connect}).
     *
     * @param query the query to execute.
     * @return a completable future.
     */
    private CompletableFuture<Response> sendQuery(Query query) {
        // check if response pump is running
        if (!exec.isShutdown() && !exec.isTerminated()) {
            final CompletableFuture<Response> awaiter = new CompletableFuture<>();
            awaiters.put(query.token, awaiter);
            try {
                lock.lock();
                if (socket == null) {
                    throw new ReqlDriverError("No socket available.");
                }
                socket.write(query.serialize());
                return awaiter.toCompletableFuture();
            } finally {
                lock.unlock();
            }
        }

        // shouldn't be here
        throw new ReqlDriverError("Can't write query because response pump is not running.");
    }

    /**
     * Writes a query without waiting for a response
     *
     * @param query the query to execute.
     */
    private void runQueryNoreply(Query query) {
        // check if response pump is running
        if (!exec.isShutdown() && !exec.isTerminated()) {
            try {
                lock.lock();
                if (socket == null) {
                    throw new ReqlDriverError("No socket available.");
                }
                socket.write(query.serialize());
                return;
            } finally {
                lock.unlock();
            }
        }

        // shouldn't be here
        throw new ReqlDriverError("Can't write query because response pump is not running.");
    }

    private <T> Flux<T> runQuery(Query query, @Nullable TypeReference<T> typeRef) {
        return Mono.fromFuture(sendQuery(query)).onErrorMap(ReqlDriverError::new)
            .flatMapMany(res -> Flux.create(new ResponseHandler<>(this, query, res, typeRef)));
    }

    private long newToken() {
        return nextToken.incrementAndGet();
    }

    // unused for some reason
    public void noreplyWait() {
        runQuery(Query.noreplyWait(newToken()), null);
    }

    private void setDefaultDB(OptArgs globalOpts) {
        if (!globalOpts.containsKey("db") && dbname != null) {
            // Only override the db global arg if the user hasn't
            // specified one already and one is specified on the connection
            globalOpts.with("db", dbname);
        }
        if (globalOpts.containsKey("db")) {
            // The db arg must be wrapped in a db ast object
            globalOpts.with("db", new Db(Arguments.make(globalOpts.get("db"))));
        }
    }

    public <T> Flux<T> run(ReqlAst term, OptArgs globalOpts, @Nullable TypeReference<T> typeRef) {
        setDefaultDB(globalOpts);
        Query q = Query.start(newToken(), term, globalOpts);
        if (globalOpts.containsKey("noreply")) {
            throw new ReqlDriverError("Don't provide the noreply option as an optarg. Use `.runNoReply` instead of `.run`");
        }
        return runQuery(q, typeRef);
    }

    public void runNoReply(ReqlAst term, OptArgs globalOpts) {
        setDefaultDB(globalOpts);
        globalOpts.with("noreply", true);
        runQueryNoreply(Query.start(newToken(), term, globalOpts));
    }

    CompletableFuture<Response> continueResponse(long token) {
        return sendQuery(Query.continue_(token));
    }

    void stop(long token) {
        // While the server does reply to the stop request, we ignore that reply.
        // This works because the response pump in `connect` ignores replies for which
        // no waiter exists.
        runQueryNoreply(Query.stop(token));
    }

    Set<ResponseHandler<?>> tracked = ConcurrentHashMap.newKeySet();

    public void loseTrackOf(ResponseHandler<?> r) {
        tracked.add(r);
    }

    public void keepTrackOf(ResponseHandler<?> r) {
        tracked.remove(r);
    }

    /**
     * Connection.Builder should be used to build a Connection instance.
     */
    public static class Builder implements Cloneable {
        private @Nullable String hostname;
        private @Nullable Integer port;
        private @Nullable String dbname;
        private @Nullable InputStream certFile;
        private @Nullable SSLContext sslContext;
        private @Nullable Long timeout;
        private @Nullable String authKey;
        private @Nullable String user;
        private @Nullable String password;

        public Builder clone() throws CloneNotSupportedException {
            Builder c = (Builder) super.clone();
            c.hostname = hostname;
            c.port = port;
            c.dbname = dbname;
            c.certFile = certFile;
            c.sslContext = sslContext;
            c.timeout = timeout;
            c.authKey = authKey;
            c.user = user;
            c.password = password;
            return c;
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
            certFile = val;
            return this;
        }

        public Builder sslContext(SSLContext val) {
            sslContext = val;
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
