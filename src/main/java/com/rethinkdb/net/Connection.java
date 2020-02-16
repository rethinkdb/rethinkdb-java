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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    protected final AtomicLong nextToken = new AtomicLong();
    protected final Set<ResponseHandler<?>> tracked = ConcurrentHashMap.newKeySet();
    protected final Lock writeLock = new ReentrantLock();

    protected @Nullable String dbname;
    protected @Nullable ConnectionSocket socket;
    protected @Nullable ResponsePump pump;

    public Connection(Builder c) {
        if (c.authKey != null && c.user != null) {
            throw new ReqlDriverError("Either `authKey` or `user` can be used, but not both.");
        }
        this.socketFactory = c.socketFactory != null ? c.socketFactory : DefaultConnectionFactory.INSTANCE;
        this.pumpFactory = c.pumpFactory != null ? c.pumpFactory : DefaultConnectionFactory.INSTANCE;
        this.hostname = c.hostname != null ? c.hostname : "localhost";
        this.port = c.port != null ? c.port : 28015;
        this.dbname = c.dbname;
        this.sslContext = c.sslContext;
        this.timeout = c.timeout;
        this.user = c.user != null ? c.user : "admin";
        this.password = c.password != null ? c.password : c.authKey != null ? c.authKey : "";
        this.unwrapLists = c.unwrapLists;
    }

    public @Nullable String db() {
        return dbname;
    }

    public void use(String db) {
        dbname = db;
    }

    public boolean isOpen() {
        return socket != null && socket.isOpen() && pump != null;
    }

    public Connection connect() {
        if (socket != null) {
            throw new ReqlDriverError("Client already connected!");
        }
        ConnectionSocket socket = socketFactory.newSocket(hostname, port, sslContext, timeout);
        this.socket = socket;

        // execute RethinkDB handshake
        HandshakeProtocol handshake = HandshakeProtocol.start(user, password);

        // initialize handshake
        ByteBuffer toWrite = handshake.toSend();
        // Sit in the handshake until it's completed. Exceptions will be thrown if
        // anything goes wrong.
        while (!handshake.isFinished()) {
            if (toWrite != null) {
                socket.write(toWrite);
            }
            String serverMsg = socket.readCString(timeout);
            handshake = handshake.nextState(serverMsg);
            toWrite = handshake.toSend();
        }

        pump = pumpFactory.newPump(socket);
        return this;
    }

    public Connection reconnect() {
        return reconnect(false);
    }

    public Connection reconnect(boolean noreplyWait) {
        close(noreplyWait);
        connect();
        return this;
    }

    public void noreplyWait() {
        runQuery(Query.noreplyWait(nextToken.incrementAndGet()), null);
    }

    public <T> Flux<T> run(ReqlAst term, OptArgs optArgs, @Nullable TypeReference<T> typeRef) {
        handleOptArgs(optArgs);
        Query q = Query.start(nextToken.incrementAndGet(), term, optArgs);
        if (optArgs.containsKey("noreply")) {
            throw new ReqlDriverError("Don't provide the noreply option as an optarg. Use `.runNoReply` instead of `.run`");
        }
        return runQuery(q, typeRef);
    }

    public void runNoReply(ReqlAst term, OptArgs optArgs) {
        handleOptArgs(optArgs);
        optArgs.with("noreply", true);
        runQueryNoreply(Query.start(nextToken.incrementAndGet(), term, optArgs));
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
            for (ResponseHandler<?> handler : tracked) {
                try {
                    handler.onConnectionClosed();
                } catch (InterruptedException ignored) {
                }
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

    // package-private methods

    protected void sendStop(long token) {
        // While the server does reply to the stop request, we ignore that reply.
        // This works because the response pump in `connect` ignores replies for which
        // no waiter exists.
        runQueryNoreply(Query.stop(token));
    }

    protected Mono<Response> sendContinue(long token) {
        return sendQuery(Query.continue_(token));
    }

    protected void loseTrackOf(ResponseHandler<?> r) {
        tracked.add(r);
    }

    protected void keepTrackOf(ResponseHandler<?> r) {
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
    protected Mono<Response> sendQuery(Query query) {
        if (socket == null || !socket.isOpen()) {
            throw new ReqlDriverError("Client not connected.");
        }
        
        if (pump == null) {
            throw new ReqlDriverError("Response pump is not running.");
        }

        Mono<Response> response = pump.await(query.token);
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

    protected <T> Flux<T> runQuery(Query query, @Nullable TypeReference<T> typeRef) {
        return sendQuery(query).onErrorMap(ReqlDriverError::new)
            .flatMapMany(res -> Flux.create(new ResponseHandler<>(this, query, res, typeRef)))
            .cache();
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
        private boolean unwrapLists = false;

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
