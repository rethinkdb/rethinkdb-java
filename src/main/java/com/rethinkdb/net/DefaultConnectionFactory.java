package com.rethinkdb.net;

import com.rethinkdb.gen.exc.ReqlDriverError;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The default {@link ConnectionSocket.Factory} and {@link ResponsePump.Factory} for any default connections.
 */
public class DefaultConnectionFactory implements ConnectionSocket.AsyncFactory, ResponsePump.Factory {
    public static final DefaultConnectionFactory INSTANCE = new DefaultConnectionFactory();

    private DefaultConnectionFactory() {
    }

    @Override
    public @NotNull CompletableFuture<ConnectionSocket> newSocketAsync(@NotNull String hostname,
                                                                       int port,
                                                                       @Nullable SSLContext sslContext,
                                                                       @Nullable Long timeoutMs) {
        return CompletableFuture.supplyAsync(() -> new SocketWrapper(hostname, port, sslContext, timeoutMs).connect());
    }

    @Override
    public @NotNull ResponsePump newPump(@NotNull ConnectionSocket socket, boolean daemonThreads) {
        return new ThreadResponsePump(socket, daemonThreads);
    }

    private static class SocketWrapper implements ConnectionSocket {
        // networking stuff
        private Socket socket;
        private InputStream inputStream;
        private OutputStream outputStream;

        // options
        private final SSLContext sslContext;
        private final Long timeoutMs;
        private final String hostname;
        private final int port;

        SocketWrapper(String hostname,
                      int port,
                      SSLContext sslContext,
                      Long timeoutMs) {
            this.hostname = hostname;
            this.port = port;
            this.sslContext = sslContext;
            this.timeoutMs = timeoutMs;
        }

        SocketWrapper connect() {
            try {
                // establish connection
                final InetSocketAddress addr = new InetSocketAddress(hostname, port);
                socket = SocketFactory.getDefault().createSocket();
                socket.connect(addr, timeoutMs == null ? 0 : timeoutMs.intValue());
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);

                // should we secure the connection?
                if (sslContext != null) {
                    try (
                      SSLSocket sslSocket = (SSLSocket) sslContext.getSocketFactory()
                        .createSocket(
                          socket,
                          socket.getInetAddress().getHostAddress(),
                          socket.getPort(),
                          true
                        )
                    ) {

                        // replace input/output streams
                        inputStream = new DataInputStream(sslSocket.getInputStream());
                        outputStream = sslSocket.getOutputStream();

                        // execute SSL handshake
                        sslSocket.startHandshake();
                    }
                } else {
                    outputStream = socket.getOutputStream();
                    inputStream = socket.getInputStream();
                }
            } catch (IOException e) {
                throw new ReqlDriverError("Connection timed out.", e);
            }
            return this;
        }

        @Override
        public void write(@NotNull ByteBuffer buffer) {
            try {
                buffer.flip();
                outputStream.write(buffer.array());
            } catch (IOException e) {
                throw new ReqlDriverError(e);
            }
        }

        @Override
        public @NotNull String readCString(@Nullable Long deadline) {
            Long timeout = deadline == null ? null : System.currentTimeMillis() + deadline;
            final StringBuilder b = new StringBuilder();
            int has;
            int next;
            char c;
            while (timeout == null || System.currentTimeMillis() < timeout) {
                try {
                    has = inputStream.available();
                    if (has < 0) {
                        break;
                    }
                    if (has == 0) {
                        Thread.yield();
                        continue;
                    }
                    if ((next = inputStream.read()) == -1 || (c = (char) next) == '\0') {
                        return b.toString();
                    }
                } catch (IOException e) {
                    throw new ReqlDriverError(e);
                }
                b.append(c);
            }
            throw new ReqlDriverError("Read timed out.");
        }

        @Override
        public @NotNull ByteBuffer read(int length) {
            try {
                byte[] buf = new byte[length];
                int bytesRead = 0;
                while (bytesRead < length) {
                    final int res = this.inputStream.read(buf, bytesRead, length - bytesRead);
                    if (res == -1) {
                        throw new ReqlDriverError("Reached the end of the read stream.");
                    } else {
                        bytesRead += res;
                    }
                }
                return ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            } catch (IOException e) {
                throw new ReqlDriverError(e);
            }
        }

        @Override
        public boolean isOpen() {
            return socket != null && (socket.isConnected() && !socket.isClosed());
        }

        @Override
        public void close() {
            // if needed, disconnect from server
            if (socket != null && isOpen()) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new ReqlDriverError(e);
                }
            }
        }

        @Override
        public String toString() {
            return "ConnectionSocket(" + hostname + ':' + port + ')';
        }
    }

    private static class ThreadResponsePump implements ResponsePump {
        private final AtomicReference<Throwable> shutdownReason = new AtomicReference<>();
        private final Thread thread;
        private Map<Long, CompletableFuture<Response>> awaiting = new ConcurrentHashMap<>();

        public ThreadResponsePump(ConnectionSocket socket, boolean daemon) {
            this.thread = new Thread(() -> {
                // pump responses until interrupted
                while (true) {
                    // validate socket is open
                    if (!socket.isOpen()) {
                        shutdown(new IOException("Socket closed, exiting response pump."));
                        return;
                    }

                    if (awaiting == null) {
                        return;
                    }

                    // read response and send it to whoever is waiting, if anyone
                    try {
                        CompletableFuture.supplyAsync(Response.readFromSocket(socket)).handle((response, t) -> {
                            if (t != null) {
                                shutdown(t);
                            } else {
                                final CompletableFuture<Response> awaiter = awaiting.remove(response.token);
                                if (awaiter != null) {
                                    awaiter.complete(response);
                                }
                            }
                            return null;
                        });
                    } catch (Exception e) {
                        shutdown(e);
                        return;
                    }
                }
            }, "RethinkDB-" + socket + "-ResponsePump");
            thread.setDaemon(daemon);
            thread.start();
        }

        @Override
        public @NotNull CompletableFuture<Response> await(long token) {
            if (awaiting == null) {
                throw new ReqlDriverError("Response pump closed.", shutdownReason.get());
            }
            CompletableFuture<Response> future = new CompletableFuture<>();
            awaiting.put(token, future);
            return future;
        }

        @Override
        public boolean isAlive() {
            return thread.isAlive();
        }

        private void shutdown(Throwable t) {
            Map<Long, CompletableFuture<Response>> awaiting = this.awaiting;
            this.shutdownReason.compareAndSet(null, t);
            this.awaiting = null;
            thread.interrupt();
            if (awaiting != null) {
                awaiting.forEach((token, future) -> future.completeExceptionally(t));
            }
        }

        @Override
        public void shutdownPump() {
            shutdown(new Throwable("Shutdown was requested."));
        }

        @Override
        public String toString() {
            return "ThreadResponsePump";
        }
    }
}
