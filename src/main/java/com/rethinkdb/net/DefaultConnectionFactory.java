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

/**
 * The default {@link ConnectionSocket.Factory} and {@link ResponsePump.Factory} for any default connections.
 */
public class DefaultConnectionFactory implements ConnectionSocket.Factory, ResponsePump.Factory {
    public static final DefaultConnectionFactory INSTANCE = new DefaultConnectionFactory();

    private DefaultConnectionFactory() {
    }

    @Override
    public @NotNull ConnectionSocket newSocket(@NotNull String hostname, int port, SSLContext sslContext, Long timeoutMs) {
        SocketWrapper s = new SocketWrapper(hostname, port, sslContext, timeoutMs);
        s.connect();
        return s;
    }

    @Override
    public @NotNull ResponsePump newPump(@NotNull ConnectionSocket socket) {
        return new ThreadResponsePump(socket);
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

        void connect() {
            try {
                // establish connection
                final InetSocketAddress addr = new InetSocketAddress(hostname, port);
                socket = SocketFactory.getDefault().createSocket();
                socket.connect(addr, timeoutMs == null ? 0 : timeoutMs.intValue());
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);

                // should we secure the connection?
                if (sslContext != null) {
                    SSLSocket sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket(
                        socket,
                        socket.getInetAddress().getHostAddress(),
                        socket.getPort(),
                        true);

                    // replace input/output streams
                    inputStream = new DataInputStream(sslSocket.getInputStream());
                    outputStream = sslSocket.getOutputStream();

                    // execute SSL handshake
                    sslSocket.startHandshake();
                } else {
                    outputStream = socket.getOutputStream();
                    inputStream = socket.getInputStream();
                }
            } catch (IOException e) {
                throw new ReqlDriverError("Connection timed out.", e);
            }
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
            try {
                final StringBuilder sb = new StringBuilder();
                int next;
                char c;
                while ((next = inputStream.read()) != -1 && (c = (char) next) != '\0') {
                    // is there a deadline?
                    if (deadline != null) {
                        // have we timed-out?
                        if (deadline < System.currentTimeMillis()) { // reached time-out
                            throw new ReqlDriverError("Connection timed out.");
                        }
                    }
                    sb.append(c);
                }

                return sb.toString();
            } catch (IOException e) {
                throw new ReqlDriverError(e);
            }
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
        private final Thread thread;
        private Map<Long, CompletableFuture<Response>> awaiting = new ConcurrentHashMap<>();

        public ThreadResponsePump(ConnectionSocket socket) {
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
                        final Response response = Response.readFromSocket(socket);
                        final CompletableFuture<Response> awaiter = awaiting.remove(response.token);
                        if (awaiter != null) {
                            awaiter.complete(response);
                        }
                    } catch (Exception e) {
                        shutdown(e);
                        return;
                    }
                }
            }, "RethinkDB-" + socket + "-ResponsePump");
            thread.start();
        }

        @Override
        public @NotNull CompletableFuture<Response> await(long token) {
            if (awaiting == null) {
                throw new ReqlDriverError("Response pump closed.");
            }
            CompletableFuture<Response> future = new CompletableFuture<>();
            awaiting.put(token, future);
            return future;
        }

        @Override
        public boolean isAlive() {
            return thread.isAlive();
        }

        private void shutdown(Exception e) {
            Map<Long, CompletableFuture<Response>> awaiting = this.awaiting;
            this.awaiting = null;
            thread.interrupt();
            if (awaiting != null) {
                awaiting.forEach((token, future) -> future.completeExceptionally(e));
            }
        }

        @Override
        public void shutdownPump() {
            shutdown(new ReqlDriverError("Response pump closed."));
        }
    }
}
